// Copyright 2017 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;

namespace Google.Cloud.BigQuery.V2
{
    public sealed partial class BigQueryCommand : DbCommand
    {
        internal CancellationTokenSource SynchronousCancellationTokenSource { get; } = new CancellationTokenSource();

        /// <inheritdoc />
        public override void Cancel()
        {
            SynchronousCancellationTokenSource.Cancel();
        }

        /// <inheritdoc />
        public override object ExecuteScalar() => ExecuteScalarAsync(SynchronousCancellationTokenSource.Token)
            .ResultWithUnwrappedExceptions();

        /// <summary>
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
            => ExecuteScalarAsync<object>(cancellationToken);

        /// <summary>
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<T> ExecuteScalarAsync<T>(CancellationToken cancellationToken)
        {
            using (var reader = await ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false))
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && reader.HasRows &&
                    reader.FieldCount > 0)
                {
                    return reader.GetFieldValue<T>(0);
                }
            }
            return default(T);
        }

        /// <inheritdoc />
        public override int ExecuteNonQuery()
            => ExecuteNonQueryAsync(SynchronousCancellationTokenSource.Token).ResultWithUnwrappedExceptions();

        /// <inheritdoc />
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            AssertCommandOptions();
            var job = await CommandOptions.CreateBigQueryJobFunc(this, cancellationToken).ConfigureAwait(false);
            await job.PollUntilCompletedAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            return 0;
        }

        /// <inheritdoc />
        public override void Prepare()
        {
            //BigQuery does not support preoptimized queries nor 2 phase commit transactions.
        }

        /// <inheritdoc />
        public override string CommandText
        {
            get => CommandOptions.CommandText;
            set => CommandOptions.CommandText = value;
        }

        /// <inheritdoc />
        public override int CommandTimeout { get; set; }
        /// <inheritdoc />
        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new NotSupportedException("BigQuery only supports CommandType.Text.");
                }
            }
        }

        /// <summary>
        /// </summary>
        public BigQueryCommandType BigQueryCommandType { get; internal set; } = BigQueryCommandType.Sql;

        /// <inheritdoc />
        public override UpdateRowSource UpdatedRowSource
        {
            get => UpdateRowSource.None;
            set
            {
                if (value != UpdateRowSource.None)
                {
                    throw new NotSupportedException(
                        "Cloud BigQuery does not support updating datasets on update/insert queries."
                        + " Please use UUIDs instead of auto increment columns, which can be created on the client.");
                }
            }
        }

        /// <summary>
        /// </summary>
        public BigQueryConnection BigQueryConnection
        {
            get => CommandOptions.BigQueryConnection;
            set => CommandOptions.BigQueryConnection = value;
        }

        /// <summary>
        /// </summary>
        public BigQueryCommandOptions CommandOptions { get; set; } = new SqlCommandOptions();

        /// <inheritdoc />
        protected override DbConnection DbConnection
        {
            get => BigQueryConnection;
            set => BigQueryConnection = (BigQueryConnection)value;
        }

        /// <inheritdoc />
        protected override DbParameterCollection DbParameterCollection => Parameters;
        /// <inheritdoc />
        protected override DbTransaction DbTransaction { get; set; }
        /// <inheritdoc />
        public override bool DesignTimeVisible { get; set; }

        /// <inheritdoc />
        protected override DbParameter CreateDbParameter()
        {
            return new BigQueryParameter();
        }

        /// <summary>
        /// </summary>
        public new Task<BigQueryDataReader> ExecuteReaderAsync()
            => ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);

        /// <summary>
        /// </summary>
        public new async Task<BigQueryDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return (BigQueryDataReader)await ExecuteDbDataReaderAsync(behavior, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            ValidateCommandBehavior(behavior);
            return ExecuteDbDataReaderAsync(
                    behavior,
                    SynchronousCancellationTokenSource.Token)
                .ResultWithUnwrappedExceptions();
        }


        /// <inheritdoc />
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
        {
            AssertCommandOptions();
            var job = await CommandOptions.CreateBigQueryJobFunc(this, cancellationToken).ConfigureAwait(false);
            return new BigQueryDataReader(job);
        }

        /// <summary>
        /// </summary>
        public Task<BigQueryJob> StartAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            AssertCommandOptions();
            return CommandOptions.CreateBigQueryJobFunc(this, cancellationToken);
        }

        private void AssertCommandOptions()
        {
            // There must be a valid and open connection.
            if (CommandOptions == null)
            {
                throw new InvalidOperationException(
                    $"{nameof(CommandOptions)} must not be null.");
            }
        }

        private void ValidateCommandBehavior(CommandBehavior behavior)
        {
            if ((behavior & CommandBehavior.KeyInfo) == CommandBehavior.KeyInfo)
            {
                throw new NotSupportedException(
                    $"{nameof(CommandBehavior.KeyInfo)} is not supported by Cloud BigQuery.");
            }
            if ((behavior & CommandBehavior.SchemaOnly) == CommandBehavior.SchemaOnly)
            {
                throw new NotSupportedException(
                    $"{nameof(CommandBehavior.SchemaOnly)} is not supported by Cloud BigQuery.");
            }
        }
    }
}
