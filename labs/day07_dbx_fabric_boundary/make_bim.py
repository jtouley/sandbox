"""Build a Power BI model.bim that carries the Panel 3 measure types plus one RLS role."""
import json
import os

M = {
    "Total Sales": "SUM ( Sales[Amount] )",
    "Total Cost": "SUM ( Sales[Cost] )",
    "Order Count": "COUNTROWS ( Sales )",
    "Distinct Customers": "DISTINCTCOUNT ( Sales[CustomerKey] )",
    "Margin %": "DIVIDE ( [Total Sales] - [Total Cost], [Total Sales] )",
    "Sales YTD": "TOTALYTD ( [Total Sales], 'Calendar'[Date] )",
    "Sales LY": "CALCULATE ( [Total Sales], SAMEPERIODLASTYEAR ( 'Calendar'[Date] ) )",
    "Sales YoY %": "DIVIDE ( [Total Sales] - [Sales LY], [Sales LY] )",
    "% of Total Sales": "DIVIDE ( [Total Sales], CALCULATE ( [Total Sales], ALL ( Product ) ) )",
    "Online Sales": "CALCULATE ( [Total Sales], Sales[Channel] = \"Online\" )",
    "Sales Rank": "RANKX ( ALL ( Product[Category] ), [Total Sales] )",
}


def entity(name, entity_name):
    return [{"name": name, "mode": "import",
             "source": {"type": "entity", "entityName": entity_name, "schemaName": "gold"}}]


bim = {"name": "gold_sales", "compatibilityLevel": 1702, "model": {"culture": "en-US", "tables": [
    {"name": "Sales", "columns": [
        {"name": "SalesKey", "dataType": "int64", "sourceColumn": "sales_key", "isKey": True},
        {"name": "OrderDate", "dataType": "dateTime", "sourceColumn": "order_date"},
        {"name": "CustomerKey", "dataType": "int64", "sourceColumn": "customer_key"},
        {"name": "ProductKey", "dataType": "int64", "sourceColumn": "product_key"},
        {"name": "Channel", "dataType": "string", "sourceColumn": "channel"},
        {"name": "Region", "dataType": "string", "sourceColumn": "region"},
        {"name": "Amount", "dataType": "decimal", "sourceColumn": "amount"},
        {"name": "Cost", "dataType": "decimal", "sourceColumn": "cost"}],
     "partitions": entity("Sales", "fact_sales"),
     "measures": [{"name": k, "expression": v} for k, v in M.items()]},
    {"name": "Product", "columns": [
        {"name": "ProductKey", "dataType": "int64", "sourceColumn": "product_key", "isKey": True},
        {"name": "Category", "dataType": "string", "sourceColumn": "category"}],
     "partitions": entity("Product", "dim_product")},
    {"name": "Calendar", "dataCategory": "Time", "columns": [
        {"name": "Date", "dataType": "dateTime", "sourceColumn": "date", "isKey": True}],
     "partitions": entity("Calendar", "dim_date")}],
    "relationships": [
        {"name": "s_p", "fromTable": "Sales", "fromColumn": "ProductKey", "toTable": "Product", "toColumn": "ProductKey"},
        {"name": "s_d", "fromTable": "Sales", "fromColumn": "OrderDate", "toTable": "Calendar", "toColumn": "Date"}],
    "roles": [{"name": "Region West", "modelPermission": "read",
               "tablePermissions": [{"name": "Sales", "filterExpression": "Sales[Region] = \"West\""}]}],
}}

if __name__ == "__main__":
    json.dump(bim, open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "gold_sales.bim"), "w"), indent=1)
    print("wrote gold_sales.bim with", len(M), "measures")
