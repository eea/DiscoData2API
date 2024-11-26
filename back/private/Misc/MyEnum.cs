namespace DiscoData2API_Priv.Misc
{
    public class MyEnum
    {
        public enum Collection
        {
            discodata_queries,
        }

        public enum JobStatus
        {
            COMPLETED,
            CANCELED,
            FAILED,
            RUNNING,
            ENQUEUED,
            STARTING,
            PLANNING,
            METADATA_RETRIEVAL,
            QUEUED,
            STARTING_PLANNING,
            STARTING_EXECUTION,
            EXECUTION_PLANNING,
            EXECUTING,
            STARTING_CLEANUP,
            COMPLETED_WITH_ERRORS,
            CANCELLATION_REQUESTED,
            CANCELLED,
            UNKNOWN
        }
    }
}