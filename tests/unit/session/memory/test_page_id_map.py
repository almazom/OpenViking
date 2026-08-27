# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

from openviking.session.memory.page_id_map import PageIdMap


class TestPageIdMap:
    def test_get_page_id_assigns_incremental_ids_for_existing_pages(self):
        pim = PageIdMap()

        id1 = pim.get_page_id("viking://user/a/memories/profile.md")
        id2 = pim.get_page_id("viking://user/a/memories/preferences/topic.md")

        assert id1 == 1
        assert id2 == 2

    def test_get_page_id_returns_same_id_for_duplicate_uri(self):
        pim = PageIdMap()

        id1 = pim.get_page_id("viking://user/a/memories/profile.md")
        id2 = pim.get_page_id("viking://user/a/memories/profile.md")

        assert id1 == id2

    def test_resolve_returns_uri_for_registered_existing_page(self):
        pim = PageIdMap()

        page_id = pim.get_page_id("viking://user/a/memories/profile.md")

        assert pim.resolve(page_id) == "viking://user/a/memories/profile.md"

    def test_resolve_returns_none_for_unknown_page_id(self):
        pim = PageIdMap()

        assert pim.resolve(999) is None

    def test_register_new_page_id_resolves_uri_for_unseen_uri(self):
        pim = PageIdMap()

        pim.register_new_page_id("viking://new-item", 105)

        assert pim.resolve(105) == "viking://new-item"

    def test_get_page_id_returns_declared_page_id_for_unseen_registered_uri(self):
        pim = PageIdMap()

        pim.register_new_page_id("viking://new-item", 105)

        assert pim.get_page_id("viking://new-item") == 105

    def test_register_new_page_id_preserves_existing_primary_id_for_seen_uri(self):
        pim = PageIdMap()

        existing_id = pim.get_page_id("viking://profile.md")
        pim.register_new_page_id("viking://profile.md", 100)

        assert pim.get_page_id("viking://profile.md") == existing_id
        assert pim.resolve(existing_id) == "viking://profile.md"
        assert pim.resolve(100) == "viking://profile.md"

    def test_allocate_new_page_id_normalizes_low_requested_id(self):
        pim = PageIdMap()
        pim.get_page_id("viking://user/a/memories/profile.md")

        assert pim.allocate_new_page_id(1) == 100

    def test_allocate_new_page_id_preserves_available_new_id(self):
        pim = PageIdMap()

        assert pim.allocate_new_page_id(105) == 105
        assert pim.allocate_new_page_id(105) == 106

    def test_allocate_new_page_id_skips_registered_and_reserved_ids(self):
        pim = PageIdMap()
        pim.register_new_page_id("viking://new-item", 100)

        assert pim.allocate_new_page_id() == 101
        assert pim.allocate_new_page_id() == 102

    def test_release_new_page_id_allows_repair_to_reuse_reservation(self):
        pim = PageIdMap()

        assert pim.allocate_new_page_id(5) == 100
        pim.release_new_page_id(100)

        assert pim.allocate_new_page_id(100) == 100

    def test_release_new_page_id_does_not_release_registered_id(self):
        pim = PageIdMap()
        pim.register_new_page_id("viking://new-item", 100)

        pim.release_new_page_id(100)

        assert pim.allocate_new_page_id(100) == 101
