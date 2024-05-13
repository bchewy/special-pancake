# special-pancake

Special Pancake is a simple order match-engine algorithm utilising HEAP queues to match orders.


## Instructions to run (local development):
- Ensure venv is set up correctly on your local development machine
- BACKEND: Run the corresponding services respectively: ie: `python3 matching.py`, or `python3 reports.py` alternatively, use docker-compose to spin up all the services.
- FRONTEND: Run the following command to start the development server: `npm run dev` in `/frontend-act`


## Extra ideas/Considerations
- Making use of RabbitMQ to handle message queuing and pub/sub pattern for real-time data exchange.
- Utilising redis as a centralised in-memory cache to store orders for milliseconds latency. 
- Test-driven development (working backwards with unit tests.)

