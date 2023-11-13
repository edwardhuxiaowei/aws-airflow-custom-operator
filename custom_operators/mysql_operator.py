from __future__ import annotations
import json
from typing import TYPE_CHECKING, Any, Union
from airflow.models import Variable
from botocore.exceptions import NoCredentialsError

from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from mysql.connector.abstracts import MySQLConnectionAbstract
    from MySQLdb.connections import Connection as MySQLdbConnection
MySQLConnectionTypes = Union["MySQLdbConnection", "MySQLConnectionAbstract"]

import os
import boto3
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


variables_basic = Variable.get(key='variables_basic', deserialize_json=True)
airflow_s3_bucket = variables_basic.get("airflow_s3_bucket")

class MySqlHook(DbApiHook):

    conn_name_attr = "mysql_conn_id"
    default_conn_name = "mysql_default"
    conn_type = "mysql"
    hook_name = "MySQL"
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.connection = kwargs.pop("connection", None)
        self.local_infile = kwargs.pop("local_infile", False)

    def set_autocommit(self, conn: MySQLConnectionTypes, autocommit: bool) -> None:

        if hasattr(conn.__class__, "autocommit") and isinstance(conn.__class__.autocommit, property):
            conn.autocommit = autocommit
        else:
            conn.autocommit(autocommit)

    def get_autocommit(self, conn: MySQLConnectionTypes) -> bool:

        if hasattr(conn.__class__, "autocommit") and isinstance(conn.__class__.autocommit, property):
            return conn.autocommit
        else:
            return conn.get_autocommit()

    def _get_conn_config_mysql_client(self, conn: Connection) -> dict:
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or "",
            "host": conn.host or "localhost",
            "db": self.schema or conn.schema or "",
        }

        # check for authentication via AWS IAM
        if conn.extra_dejson.get("iam", False):
            conn_config["passwd"], conn.port = self.get_iam_token(conn)
            conn_config["read_default_group"] = "enable-cleartext-plugin"

        conn_config["port"] = int(conn.port) if conn.port else 3306

        if conn.extra_dejson.get("charset", False):
            conn_config["charset"] = conn.extra_dejson["charset"]
            if conn_config["charset"].lower() in ("utf8", "utf-8"):
                conn_config["use_unicode"] = True
        if conn.extra_dejson.get("cursor", False):
            import MySQLdb.cursors

            if (conn.extra_dejson["cursor"]).lower() == "sscursor":
                conn_config["cursorclass"] = MySQLdb.cursors.SSCursor
            elif (conn.extra_dejson["cursor"]).lower() == "dictcursor":
                conn_config["cursorclass"] = MySQLdb.cursors.DictCursor
            elif (conn.extra_dejson["cursor"]).lower() == "ssdictcursor":
                conn_config["cursorclass"] = MySQLdb.cursors.SSDictCursor
        if conn.extra_dejson.get("ssl", False):
            # SSL parameter for MySQL has to be a dictionary and in case
            # of extra/dejson we can get string if extra is passed via
            # URL parameters
            dejson_ssl = conn.extra_dejson["ssl"]
            if isinstance(dejson_ssl, str):
                dejson_ssl = json.loads(dejson_ssl)
            conn_config["ssl"] = dejson_ssl
        if conn.extra_dejson.get("ssl_mode", False):
            conn_config["ssl_mode"] = conn.extra_dejson["ssl_mode"]
        if conn.extra_dejson.get("unix_socket"):
            conn_config["unix_socket"] = conn.extra_dejson["unix_socket"]
        if self.local_infile:
            conn_config["local_infile"] = 1
        return conn_config

    def _get_conn_config_mysql_connector_python(self, conn: Connection) -> dict:
        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
            "host": conn.host or "localhost",
            "database": self.schema or conn.schema or "",
            "port": int(conn.port) if conn.port else 3306,
        }

        if self.local_infile:
            conn_config["allow_local_infile"] = True
        # Ref: https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        for key, value in conn.extra_dejson.items():
            if key.startswith("ssl_"):
                conn_config[key] = value

        return conn_config

    def get_conn(self) -> MySQLConnectionTypes:
        """
        Connection to a MySQL database.

        Establishes a connection to a mysql database
        by extracting the connection configuration from the Airflow connection.

        .. note:: By default it connects to the database via the mysqlclient library.
            But you can also choose the mysql-connector-python library which lets you connect through ssl
            without any further ssl parameters required.

        :return: a mysql connection object
        """
        conn = self.connection or self.get_connection(getattr(self, self.conn_name_attr))

        client_name = conn.extra_dejson.get("client", "mysqlclient")

        if client_name == "mysqlclient":
            import MySQLdb

            conn_config = self._get_conn_config_mysql_client(conn)
            return MySQLdb.connect(**conn_config)

        if client_name == "mysql-connector-python":
            import mysql.connector

            conn_config = self._get_conn_config_mysql_connector_python(conn)
            return mysql.connector.connect(**conn_config)

        raise ValueError("Unknown MySQL client name provided!")

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Load a tab-delimited file into a database table."""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            LOAD DATA INFILE '{tmp_file}'
            INTO TABLE {table}
            """
        )
        conn.commit()
        conn.close()

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT * INTO OUTFILE '{tmp_file}'
            FROM {table}
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _serialize_cell(cell: object, conn: Connection | None = None) -> Any:
  
        return cell

    def get_iam_token(self, conn: Connection) -> tuple[str, int]:

        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        aws_conn_id = conn.extra_dejson.get("aws_conn_id", "aws_default")
        aws_hook = AwsBaseHook(aws_conn_id, client_type="rds")
        if conn.port is None:
            port = 3306
        else:
            port = conn.port
        client = aws_hook.get_conn()
        token = client.generate_db_auth_token(conn.host, port, conn.login)
        return token, port

    def bulk_load_custom(
            self, table: str, tmp_file: str, duplicate_key_handling: str = "IGNORE", extra_options: str = ""
    ) -> None:
 
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            LOAD DATA INFILE '{tmp_file}'
            {duplicate_key_handling}
            INTO TABLE {table}
            {extra_options}
            """
        )

        cursor.close()
        conn.commit()
        conn.close()


class S3ToMySqlOperator(BaseOperator):

    template_fields: Sequence[str] = (
        "mysql_table",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
            self,
            *,
            file_path: str,
            file_name: str,
            mysql_table: str,
            mysql_duplicate_key_handling: str = "IGNORE",
            mysql_extra_options: str | None = None,
            aws_conn_id: str = "aws_default",
            mysql_conn_id: str = "mysql_default",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.file_name = file_name
        self.mysql_table = mysql_table
        self.mysql_duplicate_key_handling = mysql_duplicate_key_handling
        self.mysql_extra_options = mysql_extra_options or ""
        self.aws_conn_id = aws_conn_id
        self.mysql_conn_id = mysql_conn_id


    def execute(self, context: Context) -> None:
 
        self.log.info("Loading %s to MySql table %s...", self.file_name, self.mysql_table)

        s3 = boto3.client("s3")
        try:
            s3.download_file(airflow_s3_bucket, self.file_path + '/' + self.file_name + ".csv",
                             '/tmp/' + self.file_name + '.csv')
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

        file = f'/tmp/{self.file_name}.csv'        

        try:
            mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
            mysql.bulk_load_custom(
                table=self.mysql_table,
                tmp_file=file,
                duplicate_key_handling=self.mysql_duplicate_key_handling,
                extra_options=self.mysql_extra_options,
            )
        finally:
            # Remove file downloaded from s3 to be idempotent.
            os.remove(file)
