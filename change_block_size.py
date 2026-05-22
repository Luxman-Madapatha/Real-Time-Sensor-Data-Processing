import requests

# Change block size
response = requests.post(
    "http://localhost:8080/config/block-size",
    json={"new_size": 50, "force_process": False},
)
print(response.json())

# Get status
response = requests.get("http://localhost:8080/status")
print(response.json())
