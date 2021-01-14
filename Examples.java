public class MissingPagination {
    AmazonDynamoDBClient dynamoClient;
    DynamoDBMapper mapper;
    private AmazonCloudFormationClient cloudformation;

    public void getDynamoDbTable() {
        List<String> tables = dynamoClient.listTables().getTableNames();
        System.out.println(tables);
    }
}
