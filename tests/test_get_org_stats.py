from os.path import join

from hdx.analysis_scripts.orgs.__main__ import main
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import temp_dir


class TestGetOrgStats:
    def test_get_org_stats(self, configuration, fixtures, mock_downloads):
        with temp_dir(
            "test_get_org_stats", delete_on_success=True, delete_on_failure=False
        ) as folder:
            total_public, total_updated_by_cod, total_updated_by_script = main(
                mock_downloads, folder
            )
            assert total_public == 18212
            assert total_updated_by_cod == 229
            assert total_updated_by_script == 15997
            filename = "org_stats.csv"
            assert_files_same(join(fixtures, filename), join(folder, filename))
            filename = "total_stats.csv"
            assert_files_same(join(fixtures, filename), join(folder, filename))
