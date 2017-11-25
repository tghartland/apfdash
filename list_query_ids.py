from prettytable import PrettyTable
import boto3

athena = boto3.client("athena")

response = athena.list_named_queries()
#print(response)

table = PrettyTable(["Name", "Query ID"])
table.align["Name"] = "l"

for query_id in response["NamedQueryIds"]:
    query = athena.get_named_query(NamedQueryId=query_id)
    #print(query["NamedQuery"]["Name"], "\t\t", query_id)
    table.add_row([query["NamedQuery"]["Name"], query_id])

print(table)
        
