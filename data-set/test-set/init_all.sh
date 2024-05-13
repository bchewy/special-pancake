curl -X POST -F "file=@input_orders.csv" http://localhost:5000/orders
curl -X POST -F "file=@input_instruments.csv" http://localhost:5000/instruments
curl -X POST -F "file=@input_clients.csv" http://localhost:5000/clients
