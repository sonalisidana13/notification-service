# IDE Prompt – Event-Driven Notification Service

You are acting as a **Senior Backend Engineer** building a **production-grade, event-driven notification system**.

Follow the instructions strictly. Do not introduce unnecessary features, frameworks, or abstractions.

---

## 🧠 Context

We are building a **Notification Service** that sends notifications asynchronously using a message queue.

The system must be:
- Reliable
- Idempotent
- Retry-safe
- Easy to reason about
- Runnable fully locally using Docker
- Built only with open-source tools (no cloud dependencies)

This project is meant to demonstrate **real-world backend engineering and system design**, not a demo app.

---

## 🎯 Core Requirements

### Functional
- Send notifications via:
  - EMAIL (using SMTP / MailHog)
  - SMS / WhatsApp (mock provider – log only)
- API-based notification requests
- Template-based messages
- User notification preferences
- Notification status tracking

### Non-Functional
- Asynchronous processing
- Retry with backoff
- Dead Letter Queue (DLQ)
- Idempotent API
- At-least-once delivery semantics
- Clear separation of concerns
- Strong error handling and logging

---

## 🏗️ Architecture (MANDATORY)

The system consists of **two services**:

### 1. Notification API Service
Responsibilities:
- Accept API requests
- Validate input
- Enforce idempotency
- Persist notification metadata in DB
- Publish messages to queue

### 2. Notification Worker Service
Responsibilities:
- Consume messages from queue
- Render templates
- Deliver notifications
- Retry on failure
- Move to DLQ after max attempts

Services communicate **only via the message queue**, never directly.

---

## 🧰 Tech Constraints

Use ONLY the following categories of tools:

- Language: Java (Spring Boot)
- Database: PostgreSQL / MySQL (give proper reasoning which is used)
- Queue: RabbitMQ
- Cache: Redis (optional)
- Email: SMTP / MailHog
- Containers: Docker + Docker Compose

❌ Do NOT use:
- Cloud services (AWS/GCP/Azure)
- Kubernetes
- Paid APIs
- UI frameworks

---

## 🗃️ Data Model (REQUIRED)

Implement at minimum:

### notifications
- id (UUID, PK)
- request_id (unique idempotency key)
- recipient
- channel (EMAIL, SMS, WHATSAPP)
- category
- template_key
- payload_json
- status (CREATED, QUEUED, SENDING, SENT, FAILED, DEAD)
- scheduled_at (nullable)
- last_error
- created_at
- updated_at

### notification_attempts
- id
- notification_id
- attempt_no
- status (SUCCESS, FAIL)
- error_message
- created_at

### templates
- template_key
- channel
- subject (nullable)
- body
- variables_schema
- is_active

### user_preferences
- user_id
- category
- channel
- enabled

---

## 📡 API Contract (DO NOT CHANGE)

### Send Notification
`POST /v1/notifications/send`

Headers:
