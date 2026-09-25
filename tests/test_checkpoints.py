"""End-to-end tests for published-stream consumer checkpoints."""
from __future__ import annotations

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SENSOR_DB_PATH", db_path)
    from app import database, main

    main.app.state.db_path = db_path
    database.init_db(db_path)
    with TestClient(main.app) as c:
        c.db_path = db_path  # type: ignore[attr-defined]
        yield c


def create_session(client, sources=("a", "b"), lateness=10.0, session_id="s"):
    r = client.post(
        "/sessions",
        json={
            "session_id": session_id,
            "source_ids": list(sources),
            "allowed_lateness_seconds": lateness,
        },
    )
    assert r.status_code == 201, r.text
    return session_id


def report(client, sid, readings):
    return client.post(
        f"/sessions/{sid}/readings", json={"readings": readings}
    )


def seal(client, sid, source, final_observed_at):
    return client.post(
        f"/sessions/{sid}/sources/{source}/seal",
        json={"final_observed_at": final_observed_at},
    )


def make_checkpoint(client, sid, name):
    return client.post(f"/sessions/{sid}/checkpoints", json={"name": name})


def retention(client, sid, cutoff, dry_run=None):
    body = {"cutoff": cutoff}
    if dry_run is not None:
        body["dry_run"] = dry_run
    return client.post(f"/sessions/{sid}/retention", json=body)


def keys(rows):
    return [(x["source_id"], x["seq"]) for x in rows]


def full_stream(client, sid, limit=1000):
    out, cursor = [], None
    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/stream", params=params).json()
        out.extend(page["readings"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


def full_checkpoint_stream(client, sid, name, limit=1000):
    out, cursor = [], None
    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        page = client.get(
            f"/sessions/{sid}/checkpoints/{name}/stream", params=params
        ).json()
        out.extend(page["readings"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


# Deterministic publish ladder with lateness 0: reporting a reading for the
# currently-slowest source beyond the other source's max releases the other
# source's readings (strictly behind the new watermark).
def publish_ladder(client, sid):
    # wm undefined until both sources report; nothing published yet.
    report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
        {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
    ])
    # slowest b@:10 -> wm :10; a@:00 publishes.
    report(client, sid, [
        {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z", "value": 3}])
    # slowest a@:20 -> wm :20; b@:10 publishes (a@:20 stays on the boundary).
    report(client, sid, [
        {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 4}])
    # slowest b@:30 -> wm :30; a@:20 publishes.
    report(client, sid, [
        {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:40Z", "value": 5}])
    # slowest a@:40 -> wm :40; b@:30 publishes.
    report(client, sid, [
        {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T00:00:50Z", "value": 6}])


class TestCreateCheckpoint:
    def test_empty_stream_checkpoint(self, client):
        sid = create_session(client)
        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["session_id"] == sid
        assert body["name"] == "cp"
        assert body["stream_offset"] == 0
        assert body["last_observed_at"] is None
        assert body["last_source_id"] is None
        assert body["last_seq"] is None
        assert body["published_watermark"] is None
        assert body["created_at"]

    def test_snapshots_position_and_watermark_in_one_transaction(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        # exactly one reading published: a seq 1 @ :00, watermark :10
        state = client.get(f"/sessions/{sid}").json()
        wm = state["published_watermark"]
        assert wm == "2026-01-01T00:00:10Z"

        body = make_checkpoint(client, sid, "cp").json()
        assert body["stream_offset"] == 1
        assert body["last_observed_at"] == "2026-01-01T00:00:00Z"
        assert body["last_source_id"] == "a"
        assert body["last_seq"] == 1
        assert body["published_watermark"] == wm

    def test_idempotent_same_name_same_position(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        assert second.json() == first.json()

    def test_idempotent_when_only_watermark_moves(self, client):
        # The published watermark may advance while nothing new is published
        # (readings sitting on/beyond the boundary).  Position equality is what
        # defines idempotency; the original snapshot (watermark included) is
        # returned unchanged.
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:20Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:30Z"},
        ])  # wm :10; a@:20 sits beyond it -> still buffered, 0 published
        first = make_checkpoint(client, sid, "cp")
        assert first.json()["stream_offset"] == 0
        assert first.json()["published_watermark"] == "2026-01-01T00:00:10Z"
        # Slowest clock moves to :30 -> wm :20; a@:20 sits exactly on the new
        # boundary, so it still is not published (position unchanged).
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:40Z"}])
        state = client.get(f"/sessions/{sid}").json()
        assert state["published_watermark"] == "2026-01-01T00:00:20Z"
        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        assert second.json() == first.json()

    def test_conflict_when_position_changed(self, client):
        sid = create_session(client, sources=("a", "b"), lateness=0.0)
        # get exactly one reading published
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 1

        # advance: b@:10 publishes
        report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z"}])
        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 409
        assert "stream_offset" in r.json()["detail"]
        # the original checkpoint survives untouched
        listed = client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]
        assert len(listed) == 1
        assert listed[0]["stream_offset"] == 1
        assert listed[0]["last_seq"] == 1
        assert listed[0]["created_at"] == first.json()["created_at"]

    def test_names_scoped_per_session(self, client):
        s1 = create_session(client, session_id="s1")
        s2 = create_session(client, sources=("a",), lateness=0.0, session_id="s2")
        assert make_checkpoint(client, s1, "cp").status_code == 201
        # same name in another session is independent
        assert make_checkpoint(client, s2, "cp").status_code == 201

    def test_create_on_closed_session_snapshots_final_position(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:01Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:00Z")
        seal(client, sid, "b", "2026-01-01T00:00:01Z")
        state = client.get(f"/sessions/{sid}").json()
        assert state["closed"] is True

        # Checkpoints can still be created on a closed session; they snapshot
        # the final position, and re-creating the name stays idempotent.
        r = make_checkpoint(client, sid, "after-close")
        assert r.status_code == 201
        body = r.json()
        assert body["stream_offset"] == 2
        assert body["published_watermark"] == state["published_watermark"]
        assert make_checkpoint(client, sid, "after-close").status_code == 200
        assert client.get(
            f"/sessions/{sid}/checkpoints/after-close/stream"
        ).json()["readings"] == []

    def test_create_unknown_session_404(self, client):
        r = make_checkpoint(client, "nope", "cp")
        assert r.status_code == 404

    @pytest.mark.parametrize("name", ["", " ", "bad name", "-lead", ".lead",
                                      "_lead", "x" * 129, "na/me", "na?me"])
    def test_invalid_name_422(self, client, name):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/checkpoints", json={"name": name})
        assert r.status_code == 422, r.text

    def test_missing_name_field_422(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/checkpoints", json={})
        assert r.status_code == 422


class TestListCheckpoints:
    def test_list_empty(self, client):
        sid = create_session(client)
        body = client.get(f"/sessions/{sid}/checkpoints").json()
        assert body == {"session_id": sid, "checkpoints": []}

    def test_list_returns_all_sorted(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        for name in ("zeta", "alpha", "mid"):
            assert make_checkpoint(client, sid, name).status_code == 201
        body = client.get(f"/sessions/{sid}/checkpoints").json()
        names = [c["name"] for c in body["checkpoints"]]
        # sorted by (created_at, name); creation was sequential so it is also
        # the declaration order -- assert the documented ordering directly
        assert names == sorted(
            names, key=lambda n: (
                next(c["created_at"] for c in body["checkpoints"] if c["name"] == n),
                n,
            )
        )
        assert set(names) == {"zeta", "alpha", "mid"}
        # each listed row carries the full snapshot
        assert all("stream_offset" in c and "published_watermark" in c
                   for c in body["checkpoints"])

    def test_list_unknown_session_404(self, client):
        assert client.get("/sessions/nope/checkpoints").status_code == 404


class TestDeleteCheckpoint:
    def test_delete_existing(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp")
        r = client.delete(f"/sessions/{sid}/checkpoints/cp")
        assert r.status_code == 204
        assert r.content == b""
        assert client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"] == []
        # deleting again is a 404
        assert client.delete(f"/sessions/{sid}/checkpoints/cp").status_code == 404

    def test_delete_keeps_other_names(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp1")
        make_checkpoint(client, sid, "cp2")
        assert client.delete(f"/sessions/{sid}/checkpoints/cp1").status_code == 204
        names = [c["name"] for c in
                 client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]]
        assert names == ["cp2"]

    def test_delete_unknown_session_404(self, client):
        assert client.delete("/sessions/nope/checkpoints/cp").status_code == 404

    def test_deleted_name_can_be_recreated(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        make_checkpoint(client, sid, "cp")
        client.delete(f"/sessions/{sid}/checkpoints/cp")
        assert make_checkpoint(client, sid, "cp").status_code == 201


class TestContinueFromCheckpoint:
    def test_resume_returns_readings_after_position_only(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        # published order: a@:00/1, b@:10/1, a@:20/2, b@:30/2
        all_rows = full_stream(client, sid)
        assert keys(all_rows) == [("a", 1), ("b", 1), ("a", 2), ("b", 2)]

        cp = make_checkpoint(client, sid, "cp").json()
        assert cp["stream_offset"] == 4

        # publish one more: a@:40 is still buffered after the ladder (final
        # watermark :40, and strict-less-than publishing keeps it there).  Move
        # both clocks to :50 so the slowest-source watermark passes :40.
        report(client, sid, [
            {"source_id": "a", "seq": 4, "observed_at": "2026-01-01T00:00:50Z"},
            {"source_id": "b", "seq": 4, "observed_at": "2026-01-01T00:00:50Z"}])
        after = full_stream(client, sid)
        assert keys(after) == keys(all_rows) + [("a", 3)]

        body = client.get(f"/sessions/{sid}/checkpoints/cp/stream").json()
        assert keys(body["readings"]) == [("a", 3)]
        assert body["next_cursor"] is None
        # response embeds the immutable checkpoint snapshot
        assert body["checkpoint"] == cp
        # ...and the session's *current* watermark
        state = client.get(f"/sessions/{sid}").json()
        assert body["published_watermark"] == state["published_watermark"]

    def test_resume_tail_equals_full_stream_tail(self, client):
        sid = create_session(client, lateness=0.0)
        # checkpoint after the first published reading
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        make_checkpoint(client, sid, "cp")
        publish_ladder(client, sid)  # extra reports; duplicates are no-ops

        full = full_stream(client, sid)
        tail = full_checkpoint_stream(client, sid, "cp")
        assert tail == full[1:]
        assert keys(tail)[0] == ("b", 1)

    def test_resume_from_empty_checkpoint_returns_everything(self, client):
        sid = create_session(client, lateness=0.0)
        cp = make_checkpoint(client, sid, "start").json()
        assert cp["stream_offset"] == 0
        publish_ladder(client, sid)
        tail = full_checkpoint_stream(client, sid, "start")
        assert tail == full_stream(client, sid)
        assert keys(tail) == [("a", 1), ("b", 1), ("a", 2), ("b", 2)]

    def test_resume_pagination_uses_same_keyset_order(self, client):
        sid = create_session(client, sources=("a", "b"), lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        make_checkpoint(client, sid, "cp")  # after a@:00
        publish_ladder(client, sid)
        # walk one reading per page
        paged = full_checkpoint_stream(client, sid, "cp", limit=1)
        assert keys(paged) == [("b", 1), ("a", 2), ("b", 2)]
        # page boundaries carry the same cursor format as /stream: resume
        # exactly once and check the first page metadata.
        first = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"limit": 1}
        ).json()
        assert len(first["readings"]) == 1
        assert first["readings"][0]["source_id"] == "b"
        second = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream",
            params={"limit": 1, "cursor": first["next_cursor"]},
        ).json()
        assert keys(second["readings"]) == [("a", 2)]

    def test_checkpoint_immutable_after_later_publishes_and_close(self, client):
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:11Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:40Z", "value": 2},
        ])  # slowest a@:11 -> wm :01, nothing released yet
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:25Z", "value": 3}])
        # slowest a@:25 -> wm :15; a@:11 publishes
        cp = make_checkpoint(client, sid, "cp").json()
        assert cp["stream_offset"] == 1

        # close the session: tail publish releases a@:25 and b@:40
        seal(client, sid, "a", "2026-01-01T00:00:25Z")
        sealed = seal(client, sid, "b", "2026-01-01T00:00:40Z")
        assert sealed.json()["closed"] is True

        state = client.get(f"/sessions/{sid}").json()
        # the checkpoint snapshot did not move
        body = client.get(f"/sessions/{sid}/checkpoints/cp/stream").json()
        assert body["checkpoint"] == cp
        # resume still means "after a@:11", so exactly the later readings
        assert keys(body["readings"]) == [("a", 2), ("b", 1)]
        # the current (terminal) watermark is exposed separately
        assert body["published_watermark"] == state["published_watermark"]
        assert body["published_watermark"] != cp["published_watermark"]

        # paging after close is exhaustive and stable across repeats
        assert full_checkpoint_stream(client, sid, "cp") == body["readings"]

    def test_resume_unknown_checkpoint_404(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/checkpoints/nope/stream")
        assert r.status_code == 404

    def test_resume_unknown_session_404(self, client):
        assert client.get("/sessions/nope/checkpoints/cp/stream").status_code == 404

    def test_resume_malformed_cursor_400(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp")
        r = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream",
            params={"cursor": "%%%not-base64%%%"},
        )
        assert r.status_code == 400
        assert "cursor" in r.json()["detail"]

    def test_cursor_at_or_before_checkpoint_rejected(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)  # 4 readings published
        make_checkpoint(client, sid, "cp")  # position = reading #4
        # a /stream cursor after reading #3 (limit 3 -> next_cursor keyset #3)
        page = client.get(f"/sessions/{sid}/stream", params={"limit": 3}).json()
        cursor_before = page["next_cursor"]
        assert cursor_before is not None
        r = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream",
            params={"cursor": cursor_before},
        )
        assert r.status_code == 400
        assert "checkpoint" in r.json()["detail"]

        # a cursor positioned exactly at the checkpoint keyset is also invalid
        page4 = client.get(f"/sessions/{sid}/stream", params={"limit": 4}).json()
        assert page4["next_cursor"] is None
        # build the exact keyset cursor via a later publish: easier to encode
        # reading #4's keyset straight from its stored representation.
        from app.cursor import encode_cursor

        fourth = page4["readings"][-1]
        from app.time_utils import store_event_time, parse_event_time

        exact = encode_cursor((
            store_event_time(parse_event_time(fourth["observed_at"])),
            fourth["source_id"],
            fourth["seq"],
        ))
        r = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"cursor": exact}
        )
        assert r.status_code == 400

    def test_limit_validation(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp")
        assert client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"limit": 0}
        ).status_code == 422
        assert client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"limit": 1001}
        ).status_code == 422

    def test_quarantined_readings_never_resumed(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])  # wm :10
        make_checkpoint(client, sid, "cp")
        # far too late -> quarantine, must never appear in the resumed stream
        late = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T00:00:01Z", "value": 9}])
        assert late.json()["quarantined"] == 1
        body = client.get(f"/sessions/{sid}/checkpoints/cp/stream").json()
        assert body["readings"] == []
        q = client.get(f"/sessions/{sid}/quarantine").json()["readings"]
        assert [(x["source_id"], x["seq"]) for x in q] == [("a", 9)]

    def test_tie_break_order_preserved_on_resume(self, client):
        sid = create_session(client, sources=("a", "b", "c"), lateness=0.0)
        report(client, sid, [
            {"source_id": "c", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:01Z"},
        ])
        make_checkpoint(client, sid, "cp")  # empty stream (wm :00 boundary)
        seal(client, sid, "a", "2026-01-01T00:00:02Z")
        seal(client, sid, "b", "2026-01-01T00:00:02Z")
        seal(client, sid, "c", "2026-01-01T00:00:02Z")
        tail = full_checkpoint_stream(client, sid, "cp", limit=2)
        assert keys(tail) == [("a", 1), ("a", 2), ("c", 1), ("b", 1)]


class TestCheckpointConcurrency:
    def test_concurrent_same_name_creates_are_linearised(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        outcomes = {}
        barrier = threading.Barrier(2)

        def create(tag):
            barrier.wait()
            outcomes[tag] = make_checkpoint(client, sid, "cp").status_code

        t1 = threading.Thread(target=create, args=("one",))
        t2 = threading.Thread(target=create, args=("two",))
        t1.start(); t2.start(); t1.join(); t2.join()

        # Exactly one create wins (201); the serialised follower sees the same
        # position and gets the idempotent 200 -- never a 409 or a 500.
        assert sorted(outcomes.values()) == [200, 201]
        listed = client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]
        assert [c["name"] for c in listed] == ["cp"]


class TestIdempotencyAgainstRetention:
    """Same-name retry idempotency is keyed on the persistent publish marker.

    Retention legitimately deletes the published rows a checkpoint already
    covered, so the live position (row count + last keyset) shrinks even
    though nothing new was ever published.  Retry idempotency must not be
    derived from that live position: an unchanged publish marker means an
    unchanged checkpoint, an advanced marker means 409 -- whether or not the
    rows published since capture are still present.
    """

    def test_same_name_retry_idempotent_after_covered_rows_cleaned(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)  # 4 published: a@:00, b@:10, a@:20, b@:30
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 4
        assert first.json()["publish_marker"] == 4

        # The checkpoint is the only one, boundary b@:30: every published row
        # is at or before its captured keyset and therefore deletable.
        cleanup = retention(client, sid, "2026-12-31T00:00:00Z", False)
        assert cleanup.status_code == 200, cleanup.text
        assert cleanup.json()["deleted"] == 4
        # the buffered boundary reading (a@:40) is not a candidate; the live
        # published stream is now empty
        assert full_stream(client, sid) == []

        # The reported bug: the live stream position is now (0, null...) and
        # differs from the frozen snapshot, so the position-based check
        # returned 409.  No new reading has been published -- it must stay
        # idempotent, returning the original snapshot verbatim.
        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200, second.text
        assert second.json() == first.json()
        assert second.json()["stream_offset"] == 4
        assert second.json()["last_seq"] == 2
        # a second cleanup round in between changes nothing either
        retention(client, sid, "2026-12-31T00:00:00Z", False)
        third = make_checkpoint(client, sid, "cp")
        assert third.status_code == 200
        assert third.json() == first.json()

    def test_409_once_a_new_reading_published_even_if_it_is_removed(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)  # 4 published: a@:00..b@:30; a@:40 buffered
        first = make_checkpoint(client, sid, "cp")
        assert first.json()["publish_marker"] == 4

        # Retention wipes every row the checkpoint already covered (all 4).
        assert retention(
            client, sid, "2026-12-31T00:00:00Z", False).json()["deleted"] == 4
        assert make_checkpoint(client, sid, "cp").status_code == 200

        # A genuinely new publish: the buffered a@:40 drains when the slowest
        # clock moves past it.  The marker must advance even when the new rows
        # later disappear.  The retention API would normally protect a row
        # published after a checkpoint (its keyset is past the boundary), so
        # the disappearance is modelled directly -- the idempotency guarantee
        # must hold regardless of how the rows went missing.
        report(client, sid, [
            {"source_id": "a", "seq": 5, "observed_at": "2026-01-01T00:01:00Z",
             "value": 7},
        ])  # slowest a@:01:00 -> wm :01:00 -> a@:40 publishes
        assert keys(full_stream(client, sid)) == [("a", 3)]

        conn = sqlite3.connect(client.db_path)
        try:
            marker = conn.execute(
                "SELECT publish_marker FROM sessions WHERE session_id = ?",
                (sid,),
            ).fetchone()[0]
            conn.execute(
                "DELETE FROM readings WHERE session_id = ? AND status = 'published'",
                (sid,),
            )
            conn.commit()
        finally:
            conn.close()
        assert marker == 5
        assert full_stream(client, sid) == []

        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 409, r.text
        assert "publish_marker" in r.json()["detail"]
        # the original checkpoint, including its original created_at, survives
        listed = client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]
        assert len(listed) == 1
        assert listed[0] == first.json()

    def test_quarantine_and_watermark_only_moves_do_not_advance_marker(self, client):
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:20Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:30Z"},
        ])  # wm :10; nothing published yet
        first = make_checkpoint(client, sid, "cp")
        assert first.json()["publish_marker"] == 0

        # watermark advances while the only buffered reading sits on the
        # boundary: no publish, so the marker is unchanged -> idempotent
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:40Z"}])
        assert client.get(f"/sessions/{sid}").json()["published_watermark"] == \
            "2026-01-01T00:00:20Z"
        assert make_checkpoint(client, sid, "cp").status_code == 200

        # late readings quarantine instead of publishing: still unchanged
        late = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T00:00:01Z",
             "value": 9}])
        assert late.json()["quarantined"] == 1
        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 200
        assert r.json() == first.json()

    def test_marker_advances_through_tail_flush_on_session_close(self, client):
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:11Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:40Z"},
        ])
        first = make_checkpoint(client, sid, "cp")
        marker_before = first.json()["publish_marker"]
        # the last-source seal tail-publishes inside the seal transaction;
        # the marker must advance in that same transaction
        seal(client, sid, "a", "2026-01-01T00:00:25Z")
        seal(client, sid, "b", "2026-01-01T00:00:40Z")
        assert client.get(f"/sessions/{sid}").json()["closed"] is True

        conn = sqlite3.connect(client.db_path)
        try:
            marker_after = conn.execute(
                "SELECT publish_marker FROM sessions WHERE session_id = ?",
                (sid,),
            ).fetchone()[0]
        finally:
            conn.close()
        assert marker_after > marker_before
        assert make_checkpoint(client, sid, "cp").status_code == 409

    def test_marker_is_not_moved_by_retention(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        make_checkpoint(client, sid, "cp")
        retention(client, sid, "2026-12-31T00:00:00Z", False)
        conn = sqlite3.connect(client.db_path)
        try:
            marker = conn.execute(
                "SELECT publish_marker FROM sessions WHERE session_id = ?",
                (sid,),
            ).fetchone()[0]
        finally:
            conn.close()
        assert marker == 4


class TestCheckpointMigration:
    def test_v3_applies_to_legacy_database(self, tmp_path, monkeypatch):
        from app import database
        from app.time_utils import store_event_time
        from app.database import utc_now

        db_path = str(tmp_path / "legacy.db")
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(database.SCHEMA_V0)
            now = store_event_time(utc_now())
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids,"
                " allowed_lateness_seconds, created_at) VALUES (?, ?, 0, ?)",
                ("legacy-s", '["a","b"]', now),
            )
            conn.commit()
        finally:
            conn.close()

        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            versions = [r[0] for r in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version")]
            assert versions == [1, 2, 3, 4, 5]
            names = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "checkpoints" in names
            cols = {r[1] for r in conn.execute("PRAGMA table_info(checkpoints)")}
            assert cols == {
                "session_id", "name", "created_at", "stream_offset",
                "last_observed_at", "last_source_id", "last_seq",
                "publish_marker", "published_watermark",
            }
            # The v5 monotonic publish marker lands on sessions too.
            session_cols = {
                r[1] for r in conn.execute("PRAGMA table_info(sessions)")
            }
            assert "publish_marker" in session_cols
        finally:
            conn.close()

        # Re-running init must not re-apply v3.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 3"
            ).fetchone()[0] == 1
        finally:
            conn.close()

        # The upgraded database serves checkpoint traffic end to end.
        monkeypatch.setenv("SENSOR_DB_PATH", db_path)
        from app import main

        main.app.state.db_path = db_path
        with TestClient(main.app) as c:
            r = c.post("/sessions/legacy-s/checkpoints", json={"name": "cp"})
            assert r.status_code == 201, r.text
            assert r.json()["stream_offset"] == 0
            assert c.get(
                "/sessions/legacy-s/checkpoints/cp/stream"
            ).status_code == 200

    def test_v5_baseline_preserves_checkpoints_and_resume_boundary(
        self, tmp_path, monkeypatch
    ):
        """Upgrading a v4 database keeps checkpoints and resume keysets.

        Checkpoint rows survive untouched except for the shared baseline
        marker; resume still starts strictly after the frozen keyset.  A
        same-name retry before any post-upgrade publish is idempotent; once a
        reading is published afterwards it conflicts -- even after the rows
        are removed again.
        """
        from app import database, main
        from app.database import utc_now
        from app.time_utils import store_event_time

        db_path = str(tmp_path / "legacy-v4.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(database.SCHEMA_V0)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
            now = store_event_time(utc_now())
            for version in (1, 2, 3, 4):
                database.MIGRATIONS[version](conn, now)
            # One live session with two published readings and a checkpoint
            # captured after the second one -- the v4-era shape.
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids,"
                " allowed_lateness_seconds, published_watermark, created_at)"
                " VALUES (?, ?, 0, ?, ?)",
                ("legacy-s", '["a","b"]',
                 "2026-01-01T00:00:20Z", now),
            )
            # v2-era per-source rows (no reports recorded in this fixture;
            # the two readings below were published under a prior release).
            conn.executemany(
                "INSERT INTO session_sources (session_id, source_id,"
                " max_observed_at, first_seen_at, final_observed_at, sealed_at)"
                " VALUES (?, ?, NULL, ?, NULL, NULL)",
                [("legacy-s", "a", now), ("legacy-s", "b", now)],
            )
            for sid_, seq, ts, wm in (
                ("a", 1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:10Z"),
                ("b", 1, "2026-01-01T00:00:10Z", "2026-01-01T00:00:20Z"),
            ):
                conn.execute(
                    "INSERT INTO readings (session_id, source_id, seq,"
                    " observed_at, payload, status, first_received_at,"
                    " published_watermark, quarantined_watermark)"
                    " VALUES (?, ?, ?, ?, '1', 'published', ?, ?, NULL)",
                    ("legacy-s", sid_, seq, ts, now, wm),
                )
            conn.execute(
                "INSERT INTO checkpoints (session_id, name, created_at,"
                " stream_offset, last_observed_at, last_source_id, last_seq,"
                " published_watermark) VALUES (?, ?, ?, 2, ?, 'b', 1, ?)",
                ("legacy-s", "cp", now, "2026-01-01T00:00:10Z",
                 "2026-01-01T00:00:20Z"),
            )
            conn.commit()
        finally:
            conn.close()

        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            assert [r[0] for r in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version")] == \
                [1, 2, 3, 4, 5]
            # shared baseline = extant published count (2)
            assert conn.execute(
                "SELECT publish_marker FROM sessions WHERE session_id ="
                " 'legacy-s'").fetchone()[0] == 2
            cp = conn.execute(
                "SELECT * FROM checkpoints WHERE session_id = 'legacy-s'"
                " AND name = 'cp'").fetchone()
            assert cp["publish_marker"] == 2
            # resume boundary (the frozen keyset) and created_at survive
            assert cp["last_observed_at"] == "2026-01-01T00:00:10Z"
            assert cp["created_at"] == now
            assert cp["stream_offset"] == 2
        finally:
            conn.close()

        # Re-running init must not re-apply v5.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 5"
            ).fetchone()[0] == 1
        finally:
            conn.close()

        monkeypatch.setenv("SENSOR_DB_PATH", db_path)
        main.app.state.db_path = db_path
        with TestClient(main.app) as c:
            c.db_path = db_path  # type: ignore[attr-defined]
            # no publish since the baseline: same-name retry is idempotent...
            r = c.post("/sessions/legacy-s/checkpoints", json={"name": "cp"})
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["publish_marker"] == 2
            assert body["created_at"]
            # ...and the resume boundary is intact (nothing after b@:10 yet)
            page = c.get(
                "/sessions/legacy-s/checkpoints/cp/stream").json()
            assert page["readings"] == []

            # publish one more reading (drain the ladder cadence): the first
            # clock push only buffers (strict-less-than boundary), the second
            # publishes a@:30 and advances the marker past the checkpoint.
            posted = c.post(
                "/sessions/legacy-s/readings",
                json={"readings": [
                    {"source_id": "a", "seq": 2,
                     "observed_at": "2026-01-01T00:00:30Z", "value": 3},
                    {"source_id": "b", "seq": 2,
                     "observed_at": "2026-01-01T00:00:40Z", "value": 4},
                ]},
            )
            assert posted.status_code == 200, posted.text
            assert posted.json()["published_now"] == 0
            pushed = c.post(
                "/sessions/legacy-s/readings",
                json={"readings": [
                    {"source_id": "a", "seq": 3,
                     "observed_at": "2026-01-01T00:00:50Z", "value": 5},
                ]},
            )
            assert pushed.status_code == 200, pushed.text
            assert pushed.json()["published_now"] == 1
            assert c.post(
                "/sessions/legacy-s/checkpoints", json={"name": "cp"}
            ).status_code == 409

            # even after the newly published row is removed, it stays 409:
            # the marker, not the live rows, is the authority
            conn2 = sqlite3.connect(db_path)
            try:
                conn2.execute(
                    "DELETE FROM readings WHERE session_id = 'legacy-s'"
                    " AND status = 'published'")
                conn2.commit()
            finally:
                conn2.close()
            assert c.post(
                "/sessions/legacy-s/checkpoints", json={"name": "cp"}
            ).status_code == 409
