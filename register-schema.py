import json
import urllib.request

def register_schema():
    print("Reading schemas/order.avsc...")
    with open('schemas/order.avsc', 'r') as f:
        schema = f.read()

    payload = json.dumps({"schema": schema}).encode('utf-8')
    req = urllib.request.Request(
        "http://localhost:8081/subjects/orders-value/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST"
    )

    print("Registering schema with Schema Registry at http://localhost:8081...")
    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read())
            print("Schema registered successfully!")
            print(json.dumps(result, indent=2))
    except Exception as e:
        print("Error registering schema:", e)
        if hasattr(e, 'read'):
            print(e.read().decode('utf-8'))

if __name__ == "__main__":
    register_schema()
