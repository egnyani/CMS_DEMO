# Azure (RELI) deployment & dashboard chat — roadmap

Follow-on work from stakeholder discussion (Sarjoo, Teams): host the dashboard in the company **Azure / RELI** environment and add **plain-English questions** about the dashboard via a **side chat**. **Drill-down** (click chart → detail) is a separate track but can reuse the same data model.

---

## To do

1. **Deploy the dashboard to Azure**  
   Host the built static site (HTML, JS, CSS) using whatever pattern RELI approves, for example:
   - Azure Static Web Apps, or  
   - Azure Storage static website (+ CDN if needed), or  
   - Azure App Service serving static files only.

2. **Add a chat feature**  
   After hosting is in place, add a **chat panel** in the dashboard UI. User messages go to a **secured backend** that calls **Azure OpenAI**. API keys and endpoints live in **Azure configuration only** — not in the browser.

3. **Pick an integration shape** (decide with RELI / security)

   | Option | What it is |
   |--------|------------|
   | **A** | Static site + **Azure Functions** + Azure OpenAI (`/api/chat` proxied to Functions). |
   | **B** | **Single Azure App Service** that serves the static dashboard **and** a small **`/api/chat`** API (no Functions required). |

   Both are standard Azure patterns; choice depends on ops preference, auth model, and cost.

---

## Notes

- **Chat vs drill-down:** Drill-down is UI + filtering on embedded data; chat needs a backend + model. They can share the same dashboard snapshot sent as context to the model.
- **Current demo:** Today the dashboard can ship from Netlify; this document tracks **company Azure** as the next hosting target.
