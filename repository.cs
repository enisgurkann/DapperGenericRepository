 //
    //  Author : Enis Gürkan
    //  Dapper Generic Repository
    //

    public class DapperRepository<T> : IRepository<T> where T : class
    {
        private string _connectionString, _tableName = "";
        private int _ConnectionTimeout, _CommandTimeout;
        public IDbTransaction _transaction;
        private IDbConnection provider;
        private DatabaseType _databaseType;
        public DapperRepository(string ConnectingString, IDbTransaction transaction = null, DatabaseType databaseType = DatabaseType.MYSQL, int ConnectionTimeout = 200, int CommandTimeout = 200)
        {

            try
            {
                _connectionString = ConnectingString;
                _tableName = typeof(T).Name;
                _transaction = transaction;
                _databaseType = databaseType;
                _ConnectionTimeout = ConnectionTimeout;
                _CommandTimeout = CommandTimeout;
                if (_databaseType == DatabaseType.MYSQL)
                {
                    provider = new MySqlConnection(_connectionString);
                    DapperExtensions.DapperExtensions.SqlDialect = new DapperExtensions.Sql.MySqlDialect();
                }
                else if (_databaseType == DatabaseType.MSSQL)
                {
                    provider = new SqlConnection(_connectionString);
                    DapperExtensions.DapperExtensions.SqlDialect = new DapperExtensions.Sql.SqlServerDialect();
                }
                else if (_databaseType == DatabaseType.ORACLE)
                {
                    provider = new SqlConnection(_connectionString);
                    DapperExtensions.DapperExtensions.SqlDialect = new DapperExtensions.Sql.OracleDialect();
                }

            }
            catch (Exception ex)
            {
                string mesaj = ex.Message;
            }
        }
        public void TransactionBegin()
        {
            _transaction = provider.BeginTransaction();
        }
        public void TransactionCommit() => _transaction.Commit();

        public void TransactionRollback() => _transaction.Rollback();
        public async void AsyncExecute(string QUERY, object MODEL)
        {
            await provider.ExecuteAsync(QUERY, MODEL, _transaction, _CommandTimeout);
        }


        public void Open() => provider.Open();
        public void Close() => provider.Close();
        public void Dispose() => provider.Dispose();
        public T Get(int Id) => provider.QuerySingle<T>(CommandStandarts.Get(_databaseType, _tableName, "Id = @GetID"), new { GetID = Id });
      
        public T Get(string Query) => provider.QuerySingle<T>(CommandStandarts.Get(_databaseType, _tableName, ((Query.Length <= 0) ? "1=1" : Query)));
        public T Get(string Query, object model) => provider.QuerySingle<T>(CommandStandarts.Get(_databaseType, _tableName, ((Query.Length <= 0) ? "1=1" : Query)), model);

        public void Add(T entity, bool LastInsertId = false) => provider.Insert<T>(entity);
        public bool Add(T entity) => provider.Insert<T>(entity) > 0;
        public bool Delete(int Id) => (int)provider.Execute($"delete from {_tableName} where Id = @Id", param: new { Id }, transaction: _transaction) > 0;
      
        public bool Update(T entity) => provider.Update<T>(entity);
        public bool Update(List<T> entity) => provider.Update<List<T>>(entity, _transaction);

        public bool IsExist(Expression<Func<T, bool>> filter) => (Count(filter) > 0);
        public bool IsExist(string Query) => Count(Query) > 0;
        public bool IsExist(string Query, object model) => Count(Query, model) > 0;

        public IEnumerable<T> List() => GetAll();
        public IEnumerable<T> List(bool OrderByDesc) => provider.Query<T>($"select * from {_tableName} order by Id {(OrderByDesc ? "desc" : "asc")}");
        public IEnumerable<T> List(string Query) => provider.Query<T>($"select * from {_tableName} where 1=1 and {((Query.Length <= 0) ? "1=1" : Query)}");
        public IEnumerable<T> List(string Query, object model) => provider.Query<T>($"select * from {_tableName} where 1=1 and {((Query.Length <= 0) ? "1=1" : Query)}", model);

        public IEnumerable<T> List(int page = 0, int pagesize = 10) => provider.Query<T>($"select * from {_tableName} LIMIT {pagesize} OFFSET {page}");
        public IEnumerable<T> List(string Query, int page = 0, int pagesize = 10) => provider.Query<T>($"select * from {_tableName} where 1=1 and {((Query.Length <= 0) ? "1=1" : Query)} LIMIT {pagesize} OFFSET {page}");
        public ListReturnModel<T> PagedList(int page = 0, int pagesize = 10)
        {
            ListReturnModel<T> ReturnModel = new ListReturnModel<T>(); /*page = (page == 0) ? 1 : page;*/
            ReturnModel.List = provider.Query<T>($"select * from {_tableName} order by Id desc LIMIT {pagesize} OFFSET {page}").ToList();
            ReturnModel.CurrentPage = page;
            ReturnModel.PageCount = pagesize;
            ReturnModel.ItemCount = Count();
            return ReturnModel;
        }
        public ListReturnModel<T> PagedList(string Query, int page = 0, int pagesize = 10)
        {
            ListReturnModel<T> ReturnModel = new ListReturnModel<T>();
            try
            {
                if (Query.Length > 4)
                    Query = (Query.Trim().Substring(0, 3).ToUpper() == "AND") ? Query.Trim().Remove(0, 3) : Query;
                ReturnModel.List = provider.Query<T>($"select * from {_tableName} where 1=1 and {((Query.Length <= 0) ? "1=1" : Query)} LIMIT {pagesize} OFFSET {page}").ToList();
                ReturnModel.CurrentPage = page;
                ReturnModel.PageCount = pagesize;
                ReturnModel.ItemCount = Count(Query);
            }
            catch (Exception ex)
            {
                string mesaj = ex.Message;
            }
            return ReturnModel;
        }
        public ListReturnModel<T> PagedList(string Query, object model, int page = 0, int pagesize = 10)
        {
            ListReturnModel<T> ReturnModel = new ListReturnModel<T>();
            try
            {
                if (Query.Length > 4)
                    Query = (Query.Trim().Substring(0, 3).ToUpper() == "AND") ? Query.Trim().Remove(0, 3) : Query;
                ReturnModel.List = provider.Query<T>($"select * from {_tableName} where 1=1 and {((Query.Length <= 4) ? "1=1" : Query)} LIMIT {pagesize} OFFSET {page}", model).ToList();
                ReturnModel.CurrentPage = page;
                ReturnModel.PageCount = pagesize;
                ReturnModel.ItemCount = Count(Query, model);
            }
            catch (Exception ex)
            {
                string mesaj = ex.Message;
            }
            return ReturnModel;
        }    

        public IEnumerable<T> GetAll() => provider.Query<T>($"select * from {_tableName}");

        public int Count(string Query, object model) => provider.Query<int>($"select count(*) from {_tableName} where 1=1 and {((Query.Length <= 0) ? "1=1" : Query)} ", model, transaction: _transaction).FirstOrDefault();
        public int Count(string Query) => provider.Query<int>($"select count(*) from {_tableName} where 1=1 and {((Query.Length <= 0) ? "1=1" : Query)} ", null, transaction: _transaction).FirstOrDefault();
        public int Count() => provider.Query<int>($"select count(*) from {_tableName} ", null, transaction: _transaction).FirstOrDefault();
        public int MaxId() => provider.Query<int>($"select max(Id) from {_tableName} ", null, transaction: _transaction).FirstOrDefault();
        public bool Truncate() => (provider.Execute($"truncate {_tableName}") > 0) ? true : false;
        public bool Drop() => (provider.Execute($"drop table if exists {_tableName}") > 0) ? true : false;
        public bool Optimize() => (provider.Execute($"optimize if exists {_tableName}") > 0) ? true : false;
        public bool Lock() => (provider.Execute($"lock tables {_tableName} read") > 0) ? true : false;
        public bool UnLock() => (provider.Execute($"FLUSH TABLES WITH READ LOCK;unlock tables {_tableName}") > 0) ? true : false;
        public bool Repair() => (provider.Execute($"repair table exists {_tableName}") > 0) ? true : false;
        public IEnumerable<T> FindList(int Id) => provider.Query<T>($"select * from {_tableName} where Id = @Id", param: new { Id }, transaction: _transaction);
        public dynamic FreeDynamicQuery(string Query) => provider.Query<dynamic>(Query, null, transaction: _transaction).FirstOrDefault();
        public dynamic FreeDynamicQuery(string Query, object model) => provider.Query<dynamic>(Query, model, transaction: _transaction).FirstOrDefault();
        public T FreeQuerySingle(string Query) => provider.Query<T>(Query, null, transaction: _transaction).FirstOrDefault();
        public T FreeQuerySingle(string Query,object model) => provider.Query<T>(Query, model, transaction: _transaction).FirstOrDefault();

        public IEnumerable<T> FreeQuery(string Query) => provider.Query<T>(Query, null, transaction: _transaction, commandTimeout: _CommandTimeout);
        public int FindId(string Query) => provider.QuerySingle<int>($"select Id from {_tableName} where 1=1 and {Query}", null, transaction: _transaction);
        public int FindId(string Query, object model) => provider.QuerySingle<int>($"select Id from {_tableName} where 1=1 and {Query}", model, transaction: _transaction);
        public int PageCount(string Query, int ListCount) => (int)(Count(Query) / ListCount);

        public bool Execute(string Query) => provider.Execute(Query) > 0;
        public bool Execute(string Query, object model) => provider.Execute(Query, model, _transaction) > 0;
        public void Rollback() => _transaction.Rollback();

    }
