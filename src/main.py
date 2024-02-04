from pathlib import Path
from argparse import ArgumentParser


def validate_file(file_path: Path) -> None:
    if not file_path.is_file():
        raise TypeError("Input file doesn't exists.")

    if file_path.suffix not in [".csv"]:
        raise TypeError(f"Input file type => '{file_path.suffix}' is not supprted")

    return None


def blue_print(
    file_path: str,
    target_database: str,
    db_host: str,
    db_port: int,
    db_user: str,
    db_password: str,
    file_seperator: str,
    file_has_header: bool,
    ignore_file_errors: bool,
    n_rows_to_insert: int,
):
    # validate file path and extension
    validate_file(Path(file_path))

    # read as dataframe

    # pass to query builder to insert

    ...


def main():
    arg_parser = ArgumentParser(
        prog="file2db",
        description="%(prog)s - Transfer of data from your local file to Database, made simple.",
    )

    arg_parser.add_argument(
        "source_file",
        metavar="Source-file",
        help="Absolute path of the file to be uploaded, with suffix",
    )
    arg_parser.add_argument(
        "database",
        metavar="Target-database",
        help="Target database where the file has to be uploaded",
        choices=["mysql"],
        default="mysql",
    )

    # database parameters
    arg_parser.add_argument(
        "--host",
        help="URL or IP address representation of database host",
        required=True,
    )
    arg_parser.add_argument(
        "--port", help="Port number to connect to the database", default=3306, type=int
    )
    arg_parser.add_argument(
        "--user", help="Username for database authentication", required=True
    )
    arg_parser.add_argument(
        "--password", help="Password for database authentication", required=True
    )

    # file_parameters
    arg_parser.add_argument(
        "--seperator", default=",", help="Column seperator in the file"
    )
    arg_parser.add_argument(
        "--has_header",
        default=True,
        help="Indicate the file contains header. default value -> True",
    )
    arg_parser.add_argument(
        "--ignore_errors",
        default=False,
        help="Try to keep reading lines if some lines yield errors. default value -> False",
    )
    arg_parser.add_argument(
        "--n_rows",
        default=None,
        help="Number of rows from the file to be inserted to database. Default -> all",
    )

    # import pdb; pdb.set_trace()
    parameters = arg_parser.parse_args()

    blue_print(
        file_path=parameters.source_file,
        target_database=parameters.database,
        db_host=parameters.host,
        db_user=parameters.user,
        db_port=parameters.port,
        db_password=parameters.password,
        file_seperator=parameters.seperator,
        file_has_header=parameters.has_header,
        ignore_file_errors=parameters.ignore_errors,
        n_rows_to_insert=parameters.n_rows,
    )


if __name__ == "__main__":
    file_path = Path("/Users/santhoshsolomon/Desktop/Final Hollywood.csv")
    main()
    # main(source_file_path=file_path, file_type=".csv")
