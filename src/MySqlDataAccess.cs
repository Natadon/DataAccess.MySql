using MySql.Data.MySqlClient;
using Natadon.DataAccess.Core;
using System;
using System.Collections.Generic;
using System.Data;

namespace Natadon.DataAccess.MySql
{
    class MySqlDataAccess : IDataAccess
    {
        private readonly string connectionString;
        private MySqlCommand command;
        private MySqlConnection connection;
        private MySqlDataAdapter dataAdapter;

        public MySqlDataAccess(string ServerName, string DatabaseName, string UserName, string Password)
        {
            connectionString = string.Format("Data Source={0};port=3306;Initial Catalog={1};User Id={2};password={3}", ServerName, DatabaseName, UserName, Password);
            connection = new MySqlConnection(connectionString);
        }
        public void ExecuteQuery(string SqlStatement, List<DataAccessParameter> Params = null)
        {
            connection.Open();

            try
            {
                command = new MySqlCommand(SqlStatement, connection);

                if (Params != null)
                {
                    List<MySqlParameter> sqlParameters = getParams(Params);

                    foreach (var item in sqlParameters)
                    {
                        command.Parameters.Add(item);
                    }
                }

                command.ExecuteNonQuery();
            }
            catch(Exception ex)
            {
                throw ex;
            }
            finally
            {
                command.Dispose();

                connection.Close();
            }
        }

        public DataSet GetDataSet(string SqlStatement, List<DataAccessParameter> Params = null)
        {
            connection.Open();

            try
            {
                command = new MySqlCommand(SqlStatement, connection);

                if (Params != null)
                {
                    List<MySqlParameter> sqlParameters = getParams(Params);

                    foreach (var item in sqlParameters)
                    {
                        command.Parameters.Add(item);
                    }
                }

                dataAdapter = new MySqlDataAdapter(command);
                DataSet ds = new DataSet();

                dataAdapter.Fill(ds);

                return ds;
            }
            catch(Exception ex)
            {
                throw ex;
            }
            finally
            {
                dataAdapter.Dispose();

                connection.Close();
            }
        }

        public object GetScalar(string SqlStatement, List<DataAccessParameter> Params = null)
        {
            connection.Open();

            try
            {
                command = new MySqlCommand(SqlStatement, connection);

                if (Params != null)
                {
                    List<MySqlParameter> sqlParameters = getParams(Params);

                    foreach (var item in sqlParameters)
                    {
                        command.Parameters.Add(item);
                    }
                }

                object obj = command.ExecuteScalar();

                return obj;
            }
            catch(Exception ex)
            {
                throw ex;
            }
            finally
            {
                connection.Close();
            }
        }

        private List<MySqlParameter> getParams(List<DataAccessParameter> accessParameters)
        {
            List<MySqlParameter> retVal = new List<MySqlParameter>();

            foreach(var item in accessParameters)
            {
                retVal.Add(new MySqlParameter(item.ParameterName, item.Value));
            }

            return retVal;
        }
    }
}
