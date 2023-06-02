from os.path import join

from get_org_stats import main
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir


class TestGetOrgStats:
    def test_get_org_stats(self, configuration, fixtures, mock_downloads):
        with temp_dir(
            "test_get_org_stats", delete_on_success=True, delete_on_failure=False
        ) as folder:
            main(mock_downloads, folder)
            filename = "org_stats.csv"
            assert_files_same(join(fixtures, filename), join(folder, filename))
