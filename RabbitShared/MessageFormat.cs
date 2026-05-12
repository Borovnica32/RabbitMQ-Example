namespace RabbitShared
{
    public class MessageFormat
    {
        public int Id { get; set; }

        public required string Message { get; set; }

        public required string messageTimeStamp { get; set; }

        public required string userName { get; set; }
            
        public int MessageSizeBytes { get; set; }
    }
}
