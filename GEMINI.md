## Project Name
E-commerce Clickstream Simulation Pipeline

## Project Goal
Simulate and process synthetic e-commerce clickstream data in a **local data pipeline**.
The pipeline should generate fake user activity events (page views, add-to-cart, purchases), 
stream them through **Kafka**, process them with **Spark** (or similar), 
and load results into **PostgreSQL** for analytics and visualization (Grafana or Superset).

---

## Tech Stack and Environment
- **Languages:** Python (preferred), SQL
- **Data Stream:** Apache Kafka (via Docker Compose)
- **Processing:** Spark Structured Streaming (Python/PySpark jobs)
- **Storage:** PostgreSQL (run in Docker)
- **Orchestration:** Simple scripts / later extensible to Airflow if needed
- **Optional Dashboard:** Grafana or Apache Superset (via Docker)

### Local-First Development
- Everything should work locally via **`docker-compose up`**.
- No mandatory cloud dependencies.
- Minimize external API calls; generate **synthetic data** with `faker`.

---

## Code Style Guidelines
- Prefer **Python 3.10+** features (type hints, dataclasses where useful).
- Keep code **modular**: 
  - producer/ (event generators)
  - processor/ (Spark jobs)
  - db/ (schemas, init SQL)
  - dags/ (if adding Airflow later)
- Include **docstrings** and clear function names.
- Avoid hardcoding credentials (use `.env` file).
- Use black/flake8 style for formatting/linting.

---

## Success Criteria
1. Running `docker-compose up` should:
   - Start Kafka broker
   - Start PostgreSQL database
   - Optionally start Grafana/Superset

2. A producer script should:
   - Generate fake e-commerce clickstream events every few seconds
   - Push events into Kafka topic `clickstream.raw`

3. A Spark job should:
   - Consume from Kafka `clickstream.raw`
   - Aggregate metrics like *session length*, *conversion rate*, *cart additions*
   - Write results into PostgreSQL `analytics` schema

4. Dashboard (optional):
   - Show real-time results (e.g., conversion % trending, top products)

---

## Watch Outs for LLM
1. **Do NOT** pull real user data → only use synthetic, randomly generated events.
2. Ensure **idempotency**: pipelines can be restarted without duplicates breaking DB.
3. **Docker-compose file** must include clear networking between services (`kafka`, `spark`, `postgres`).
4. Don’t lock Spark/Kafka to specific versions unless required → allow modern stable releases.
5. Keep configuration (ports, credentials) in `.env` or configs, not hardcoded.
6. Ensure docker setup works on both Linux and macOS (avoid OS-specific paths).
7. Update the README at the same time as updating the code. Keep README concise, with **step-by-step startup instructions**.

---

## Example Data Model
**Raw events:**
```json
{
  "event_id": "uuid",
  "user_id": "uuid",
  "event_type": "page_view | add_to_cart | purchase",
  "product_id": "uuid",
  "timestamp": "2025-08-19T14:05:22Z"
}   