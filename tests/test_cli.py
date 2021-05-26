import pathlib
from unittest.mock import patch

import pytest

from sqllineage.cli import main


def test_file_exception():
    for args in (["-f", str(pathlib.Path().absolute())], ["-f", "nonexist_file"]):
        with pytest.raises(SystemExit) as e:
            main(args)
        assert e.value.code == 1


@patch("builtins.open", side_effect=PermissionError())
def test_file_permission_error(_):
    with pytest.raises(SystemExit) as e:
        main(["-f", __file__])
    assert e.value.code == 1
