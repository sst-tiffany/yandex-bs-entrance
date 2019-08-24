### Examples
#### /imports

wrong (returns schema error)
```bash
curl -X POST -H "Content-Type: application/json" -d '{}' http://192.168.0.15:8080/imports
```

correct
```bash
curl -X POST -H "Content-Type: application/json" -d '{"test": ""}' http://192.168.0.15:8080/imports
```
