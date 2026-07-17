import asyncio

from screenshot_runner import delete_from_drive, extract_rank, find_filtered_entries


class _FakeLocator:
    def __init__(self, result=None, raises=None):
        self.first = self
        self._result = result
        self._raises = raises

    async def evaluate(self, js):
        if self._raises:
            raise self._raises
        return self._result


class _FakePage:
    def __init__(self, locator):
        self._locator = locator

    def get_by_text(self, keyword, exact=False):
        return self._locator


def test_extract_rank_parses_matched_badge_text():
    page = _FakePage(_FakeLocator(result="#7"))
    assert asyncio.run(extract_rank(page, "Some Book")) == 7


def test_extract_rank_returns_none_when_no_single_match():
    page = _FakePage(_FakeLocator(result=None))
    assert asyncio.run(extract_rank(page, "Some Book")) is None


def test_extract_rank_returns_none_on_locator_error():
    page = _FakePage(_FakeLocator(raises=RuntimeError("boom")))
    assert asyncio.run(extract_rank(page, "Some Book")) is None


class _FakeExec:
    def __init__(self, response):
        self._response = response

    def execute(self):
        return self._response


class _FakeFiles:
    def __init__(self, list_response):
        self._list_response = list_response
        self.calls = []

    def list(self, **kwargs):
        self.calls.append(("list", kwargs))
        return _FakeExec(self._list_response)

    def delete(self, **kwargs):
        self.calls.append(("delete", kwargs))
        return _FakeExec({})


class _FakeDriveService:
    def __init__(self, list_response=None):
        self._files = _FakeFiles(list_response or {"files": []})

    def files(self):
        return self._files


def test_find_filtered_entries_parses_rank_and_ignores_non_matching_names():
    response = {
        "files": [
            {"id": "a", "name": "rank0007_Fiction_US_Some_Book_20260717-090000.png"},
            {"id": "b", "name": "rank0003_Fiction_US_Some_Book_20260717-100000.png"},
            {"id": "c", "name": "not_a_rank_file.png"},
        ]
    }
    drive = _FakeDriveService(response)
    entries = find_filtered_entries(drive, "folder123", "Fiction_US_Some_Book")
    assert sorted(entries) == [("a", 7), ("b", 3)]


def test_find_filtered_entries_empty_when_no_matches():
    drive = _FakeDriveService({"files": []})
    assert find_filtered_entries(drive, "folder123", "Fiction_US_Some_Book") == []


def test_delete_from_drive_calls_delete_with_supports_all_drives():
    drive = _FakeDriveService()
    delete_from_drive(drive, "file123")
    kind, kwargs = drive._files.calls[-1]
    assert kind == "delete"
    assert kwargs["fileId"] == "file123"
    assert kwargs["supportsAllDrives"] is True
