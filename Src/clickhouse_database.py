#!/usr/bin/env python3
"""
ClickHouse数据库连接和操作类
用于从ClickHouse集群中获取数据
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


class ClickHouseDataBase:
    """ClickHouse数据库操作类"""

    def __init__(
        self,
        hosts: List[str] = None,
        ports: List[int] = None,
        username: str = "default",
        password: str = "",
        database: str = "default",
        secure: bool = False,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
    ):
        """
        初始化ClickHouse数据库连接

        Args:
            hosts: ClickHouse服务器地址列表，默认 ['10.5.1.63', '10.5.1.64', '10.5.1.65']
            ports: ClickHouse服务器端口列表，默认 [9101, 9102]
            username: 用户名，默认 'default'
            password: 密码
            database: 数据库名，默认 'default'
            secure: 是否使用SSL/TLS连接
            connect_timeout: 连接超时时间（秒）
            send_receive_timeout: 发送接收超时时间（秒）
        """
        # 默认配置
        if hosts is None:
            hosts = ["10.5.1.63", "10.5.1.64", "10.5.1.65"]
        if ports is None:
            ports = [9101, 9102]

        self.hosts = hosts
        self.ports = ports
        self.username = username
        self.password = password
        self.database = database
        self.secure = secure
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout

        self.client = None
        self.current_host = None
        self.current_port = None

    def connect(self) -> bool:
        """
        连接到ClickHouse服务器
        会尝试所有host和port的组合，直到成功连接

        Returns:
            bool: 连接是否成功
        """
        try:
            import clickhouse_connect
        except ImportError:
            raise ImportError(
                "未安装 clickhouse-connect 库，请先安装: pip install clickhouse-connect"
            )

        # 尝试所有host和port的组合
        for host in self.hosts:
            for port in self.ports:
                try:
                    print(f"尝试连接到 {host}:{port}...")
                    self.client = clickhouse_connect.get_client(
                        host=host,
                        port=port,
                        username=self.username,
                        password=self.password,
                        database=self.database,
                        secure=self.secure,
                        connect_timeout=self.connect_timeout,
                        send_receive_timeout=self.send_receive_timeout,
                    )

                    # 测试连接
                    result = self.client.query("SELECT 1")
                    if result.result_rows and result.result_rows[0][0] == 1:
                        self.current_host = host
                        self.current_port = port
                        print(f"成功连接到 {host}:{port}")
                        return True

                except Exception as e:
                    print(f"连接 {host}:{port} 失败: {str(e)}")
                    continue

        print("所有连接尝试均失败")
        return False

    def disconnect(self):
        """断开数据库连接"""
        if self.client:
            self.client.close()
            self.client = None
            self.current_host = None
            self.current_port = None
            print("已断开数据库连接")

    def execute_query(
        self, query: str, parameters: Dict[str, Any] = None
    ) -> pd.DataFrame:
        """
        执行SQL查询并返回结果

        Args:
            query: SQL查询语句
            parameters: 查询参数（可选）

        Returns:
            pd.DataFrame: 查询结果

        Raises:
            ConnectionError: 如果未连接到数据库
            Exception: 查询执行错误
        """
        if not self.client:
            raise ConnectionError("未连接到数据库，请先调用 connect() 方法")

        try:
            if parameters:
                result = self.client.query(query, parameters)
            else:
                result = self.client.query(query)

            # 转换为DataFrame
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            return df

        except Exception as e:
            raise Exception(f"查询执行失败: {str(e)}")

    def execute_query_with_batches(
        self,
        query: str,
        batch_size: int = 10000,
        parameters: Dict[str, Any] = None,
    ):
        """
        执行SQL查询并分批返回结果（用于处理大量数据）

        Args:
            query: SQL查询语句
            batch_size: 每批数据大小
            parameters: 查询参数（可选）

        Yields:
            pd.DataFrame: 每批数据

        Raises:
            ConnectionError: 如果未连接到数据库
            Exception: 查询执行错误
        """
        if not self.client:
            raise ConnectionError("未连接到数据库，请先调用 connect() 方法")

        try:
            if parameters:
                result = self.client.query(query, parameters=parameters)
            else:
                result = self.client.query(query)

            # 分批返回数据
            column_names = result.column_names
            batch = []
            for row in result.result_rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    yield pd.DataFrame(batch, columns=column_names)
                    batch = []

            # 返回最后一批数据
            if batch:
                yield pd.DataFrame(batch, columns=column_names)

        except Exception as e:
            raise Exception(f"查询执行失败: {str(e)}")

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        """
        为标识符（表名、数据库名等）添加反引号，处理特殊字符

        Args:
            identifier: 标识符名称

        Returns:
            str: 用反引号包裹的标识符
        """
        # 如果已经被反引号包裹，直接返回
        if identifier.startswith("`") and identifier.endswith("`"):
            return identifier
        # 返回用反引号包裹的标识符
        return f"`{identifier}`"

    def get_databases(self) -> List[str]:
        """
        获取服务器中所有数据库名（模式名）

        Returns:
            List[str]: 数据库名列表
        """
        query = "SHOW DATABASES"
        df = self.execute_query(query)
        return df.iloc[:, 0].tolist()

    def get_tables(self, database: str = None) -> List[str]:
        """
        获取数据库中所有表名

        Args:
            database: 数据库名，默认使用初始化时指定的数据库

        Returns:
            List[str]: 表名列表
        """
        if database is None:
            database = self.database
        query = f"SHOW TABLES FROM {database}"
        df = self.execute_query(query)
        return df.iloc[:, 0].tolist()

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        获取表结构

        Args:
            table_name: 表名，可以是 "table_name" 或 "database.table_name"

        Returns:
            pd.DataFrame: 表结构信息
        """
        # 解析数据库名和表名
        if "." in table_name:
            database, table = table_name.split(".", 1)
            # 移除已有的反引号
            database = database.strip("`")
            table = table.strip("`")
            # 重新构建查询，确保每个部分都用反引号包裹
            query = f"DESCRIBE TABLE `{database}`.`{table}`"
        else:
            table = table_name.strip("`")
            query = f"DESCRIBE TABLE {self.database}.`{table}`"

        return self.execute_query(query)

    def get_table_count(self, table_name: str) -> int:
        """
        获取表中记录总数

        Args:
            table_name: 表名，可以是 "table_name" 或 "database.table_name"

        Returns:
            int: 记录总数
        """
        # 解析数据库名和表名
        if "." in table_name:
            database, table = table_name.split(".", 1)
            # 移除已有的反引号
            database = database.strip("`")
            table = table.strip("`")
            # 重新构建查询，确保每个部分都用反引号包裹
            query = f"SELECT COUNT(*) as count FROM `{database}`.`{table}`"
        else:
            table = table_name.strip("`")
            query = f"SELECT COUNT(*) as count FROM {self.database}.`{table}`"

        df = self.execute_query(query)
        return df.iloc[0, 0]

    def get_data_by_time_range(
        self,
        table_name: str,
        time_column: str,
        start_time: datetime,
        end_time: datetime,
        columns: str = "*",
        conditions: str = "",
        batch_size: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        根据时间范围获取数据

        Args:
            table_name: 表名
            time_column: 时间列名
            start_time: 开始时间
            end_time: 结束时间
            columns: 要查询的列，默认所有列
            conditions: 额外的查询条件（可选）
            batch_size: 批处理大小，如果指定则分批返回数据

        Returns:
            pd.DataFrame: 查询结果（如果指定batch_size则返回生成器）
        """
        # 格式化时间
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

        # 构建查询
        query = f"""
            SELECT {columns}
            FROM {self.database}.{table_name}
            WHERE {time_column} >= '{start_str}'
            AND {time_column} <= '{end_str}'
        """

        if conditions:
            query += f" AND {conditions}"

        # 根据是否分批返回不同类型的结果
        if batch_size:
            return self.execute_query_with_batches(query, batch_size)
        else:
            return self.execute_query(query)

    def get_local_tables(self, database: str = None) -> List[str]:
        """
        获取数据库中所有以 '_local' 结尾的表名

        Args:
            database: 数据库名，默认使用初始化时指定的数据库

        Returns:
            List[str]: _local 表名列表

        Example:
            >>> db = ClickHouseDataBase(...)
            >>> db.connect()
            >>> local_tables = db.get_local_tables()
            >>> print(local_tables)
            ['_2202_local', '_110504_local', ...]
            >>> db.disconnect()
        """
        if not self.client:
            raise ConnectionError("未连接到数据库，请先调用 connect() 方法")

        # 使用指定的数据库或默认数据库
        target_db = database if database else self.database

        # 获取所有表
        tables = self.get_tables(database=target_db)

        # 筛选 _local 结尾的表
        local_tables = [table for table in tables if table.endswith("_local")]

        return local_tables

    def query_local_table(
        self,
        table_name: str,
        database: str = None,
        batch_size: int = 10000,
        show_progress: bool = True,
    ):
        """
        查询指定 _local 表的数据，name 字段以 'SAC' 开头
        使用分页查询（LIMIT + OFFSET），以批次形式返回所有数据
        避免一次性加载所有数据到内存

        Args:
            table_name: 表名（如 '_2202_local'）
            database: 数据库名，默认使用初始化时指定的数据库
            batch_size: 每批数据的大小，默认 10000
            show_progress: 是否显示进度信息，默认 True

        Yields:
            pd.DataFrame: 每个批次的数据

        Example:
            >>> db = ClickHouseDataBase(...)
            >>> db.connect()
            >>> for batch_df in db.query_local_table('_2202_local'):
            ...     print(f"批次记录数: {len(batch_df)}")
            ...     # 处理 batch_df
            >>> db.disconnect()
        """
        if not self.client:
            raise ConnectionError("未连接到数据库，请先调用 connect() 方法")

        # 使用指定的数据库或默认数据库
        target_db = database if database else self.database

        if show_progress:
            print(f"\n正在查询表: {target_db}.{table_name}")

        try:
            # 构建带反引号的完整表名
            full_table_name = f"`{target_db}`.`{table_name}`"

            # 固定过滤条件：name 以 'SAC' 开头
            SAC_filter = "SAC"
            where_clause = f"WHERE name LIKE '{SAC_filter}%'"

            # 获取总记录数（name 以 'SAC' 开头的记录）
            count_query = f"SELECT COUNT(*) as count FROM {full_table_name} {where_clause}"
            df_count = self.execute_query(count_query)
            total_count = df_count.iloc[0, 0] if not df_count.empty else 0

            if total_count == 0:
                if show_progress:
                    print(
                        f"  表 {table_name} 中没有 name 以 '{SAC_filter}' 开头的记录"
                    )
                return

            if show_progress:
                print(f"  找到 {total_count:,} 条记录")

            # 分页查询，避免一次性加载所有数据到内存
            batch_count = 0
            offset = 0

            while offset < total_count:
                # 使用 LIMIT 和 OFFSET 进行分页查询
                query = f"SELECT * FROM {full_table_name} {where_clause} LIMIT {batch_size} OFFSET {offset}"

                # 执行查询
                result = self.client.query(query)

                # 转换为 DataFrame
                df = pd.DataFrame(result.result_rows, columns=result.column_names)

                # 如果没有数据了，退出循环
                if len(df) == 0:
                    break

                batch_count += 1

                if show_progress:
                    progress = min(offset + len(df), total_count)
                    print(
                        f"  返回批次 {batch_count}: {progress}/{total_count} 条记录"
                    )

                # 返回当前批次数据
                yield df

                # 更新 offset
                offset += len(df)

                # 如果返回的数据少于 batch_size，说明已经是最后一批
                if len(df) < batch_size:
                    break

            if show_progress:
                print(f"  表 {table_name} 查询完成，共 {batch_count} 个批次")

        except Exception as e:
            if show_progress:
                print(f"  查询表 {table_name} 失败: {str(e)}")
            raise

    def query_local_tables(
        self,
        database: str = None,
        batch_size: int = 10000,
        show_progress: bool = True,
    ):
        """
        查询指定数据库中所有 _local 表的数据，name 字段以 'SAC' 开头
        以批次形式返回所有数据

        Args:
            database: 数据库名，默认使用初始化时指定的数据库
            batch_size: 每批数据的大小，默认 10000
            show_progress: 是否显示进度信息，默认 True

        Yields:
            tuple: (table_name, dataframe) 每个批次的数据
                   - table_name: 表名
                   - dataframe: 该表的一个批次数据 (pd.DataFrame)

        Example:
            >>> db = ClickHouseDataBase(...)
            >>> db.connect()
            >>> for table_name, batch_df in db.query_local_tables():
            ...     print(f"表: {table_name}, 批次记录数: {len(batch_df)}")
            ...     # 处理 batch_df
            >>> db.disconnect()
        """
        if not self.client:
            raise ConnectionError("未连接到数据库，请先调用 connect() 方法")

        # 使用指定的数据库或默认数据库
        target_db = database if database else self.database

        # 获取所有 _local 表
        local_tables = self.get_local_tables(database=target_db)

        if not local_tables:
            if show_progress:
                print(
                    f"在数据库 '{target_db}' 中未找到以 '_local' 结尾的表。"
                )
            return

        if show_progress:
            print(f"找到 {len(local_tables)} 个 _local 表")

        # 遍历每个 _local 表
        for local_table in local_tables:
            try:
                # 使用 query_local_table 查询单个表
                for batch_df in self.query_local_table(
                    table_name=local_table,
                    database=target_db,
                    batch_size=batch_size,
                    show_progress=show_progress,
                ):
                    yield local_table, batch_df

            except Exception as e:
                if show_progress:
                    print(f"  查询表 {local_table} 失败: {str(e)}")
                continue

    def test_connection(self) -> Dict[str, Any]:
        """
        测试数据库连接并返回连接信息

        Returns:
            Dict[str, Any]: 连接信息
        """
        if not self.client:
            return {"connected": False, "error": "未连接到数据库"}

        try:
            # 获取版本信息
            result = self.client.query("SELECT version()")
            version = result.result_rows[0][0]

            # 获取当前数据库
            result = self.client.query("SELECT currentDatabase()")
            current_db = result.result_rows[0][0]

            return {
                "connected": True,
                "host": self.current_host,
                "port": self.current_port,
                "database": current_db,
                "version": version,
                "username": self.username,
            }

        except Exception as e:
            return {"connected": False, "error": str(e)}

    def __enter__(self):
        """支持with语句"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句"""
        self.disconnect()


# 使用示例
if __name__ == "__main__":
    # 配置信息
    HOSTS = ["10.5.1.63", "10.5.1.64", "10.5.1.65"]
    PORTS = [9101, 9102]
    USERNAME = "default"
    PASSWORD = "Yk2KIy72wYdkP$o^@y2lc6U8TSg%C9O001"

    # 创建数据库实例
    db = ClickHouseDataBase(
        hosts=HOSTS, ports=PORTS, username=USERNAME, password=PASSWORD
    )

    # 使用with语句自动管理连接
    with db:
        # 测试连接
        info = db.test_connection()
        print(f"连接信息: {info}")

        # 获取所有表
        tables = db.get_tables()
        print(f"数据库表: {tables}")

        # 如果有表，获取表结构
        if tables:
            for table in tables[:3]:  # 只显示前3个表
                print(f"\n表 '{table}' 的结构:")
                schema = db.get_table_schema(table)
                print(schema)

                count = db.get_table_count(table)
                print(f"总记录数: {count:,}")
