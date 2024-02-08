from os.path import join

from get_datasets_info import main
from common.compare import assert_files_same
from hdx.utilities.path import temp_dir


class TestGetDatasetsInfo:
    def test_get_datasets_info(self, configuration, fixtures, mock_downloads):
        with temp_dir(
            "test_get_datasets_info", delete_on_success=True, delete_on_failure=False
        ) as folder:
            main(mock_downloads, folder)
            filename = "datasets.csv"
            assert_files_same(join(fixtures, filename), join(folder, filename))
            filename = "non_script_updates.csv"
            assert_files_same(join(fixtures, filename), join(folder, filename))
