#if NET45 || NET451
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Logging;

namespace Google.Cloud.Spanner
{
    internal sealed class VolatileResourceManager : ISinglePhaseNotification, ISpannerTransaction, IDisposable
    {
        private readonly SpannerConnection _spannerConnection;
        private readonly TimestampBound _timestampBound;
        private SpannerTransaction _transaction;

        public VolatileResourceManager(SpannerConnection spannerConnection, TimestampBound timestampBound)
        {
            _spannerConnection = spannerConnection;
            _timestampBound = timestampBound;
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }

        public void Commit(Enlistment enlistment)
        {
            Logger.Warn(
                () =>
                    "Got a Commit call, which indicates two phase commit inside a transaction scope.  This is currently not supported in Spanner.");
            throw new NotSupportedException("Spanner only supports single phase commit (2-P Commit not supported)."
                                            +
                                            " This error can happen when attempting to use multiple transaction resources but may also happen for"
                                            + " other reasons that cause a Transaction to use two-phase commit.");
        }

        public void InDoubt(Enlistment enlistment)
        {
            Logger.Warn(
                () =>
                    "Got a InDoubt call, which indicates two phase commit inside a transaction scope.  This is currently not supported in Spanner.");
            enlistment.Done();
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            Logger.Warn(
                () =>
                    "Got a Prepare call, which indicates two phase commit inside a transaction scope.  This is currently not supported in Spanner.");
            preparingEnlistment.ForceRollback(new NotSupportedException(
                "Spanner only supports single phase commit (Prepare not supported)."
                + " This error can happen when attempting to use multiple transaction resources but may also happen for"
                + " other reasons that cause a Transaction to use two-phase commit."));
        }

        public void Rollback(Enlistment enlistment)
        {
            try
            {
                ExecuteHelper.WithErrorTranslationAndProfiling(() =>
                    _transaction?.Rollback(), "VolatileResourceManager.Rollback");
                enlistment.Done();
            }
            catch (Exception e)
            {
                Logger.Error(
                    () =>
                        "Error attempting to rollback a transaction.", e);
            }
            finally
            {
                Dispose();
            }
        }

        public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            try
            {
                ExecuteHelper.WithErrorTranslationAndProfiling(() =>
                    _transaction?.Commit(), "VolatileResourceManager.Commit");
                singlePhaseEnlistment.Committed();
            }
            catch (SpannerException e)
            {
                singlePhaseEnlistment.Aborted(e);
            }
            catch (Exception e)
            {
                singlePhaseEnlistment.InDoubt(e);
            }
            finally
            {
                Dispose();
            }
        }

        public async Task<int> ExecuteMutationsAsync(List<Mutation> mutations, CancellationToken cancellationToken)
        {
            var transaction = await GetTransactionAsync(cancellationToken);
            if (transaction == null)
                throw new InvalidOperationException("Unable to obtain a spanner transaction to execute within.");
            return await ((ISpannerTransaction) transaction).ExecuteMutationsAsync(mutations, cancellationToken);
        }

        public async Task<ReliableStreamReader> ExecuteQueryAsync(string sql, CancellationToken cancellationToken)
        {
            var transaction = await GetTransactionAsync(cancellationToken);
            if (transaction == null)
                throw new InvalidOperationException("Unable to obtain a spanner transaction to execute within.");
            return await((ISpannerTransaction) transaction).ExecuteQueryAsync(sql, cancellationToken);
        }

        private async Task<SpannerTransaction> GetTransactionAsync(CancellationToken cancellationToken)
        {
            //note that we delay transaction creation (and thereby session allocation) 
            if (_timestampBound != null)
                return _transaction ?? (_transaction =
                           await _spannerConnection.BeginReadOnlyTransactionAsync(_timestampBound, cancellationToken));
            return _transaction ?? (_transaction = await _spannerConnection.BeginTransactionAsync(cancellationToken));
        }
    }
}

#endif