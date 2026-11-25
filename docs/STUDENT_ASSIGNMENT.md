Conversation Log
ano ulož mi to do docs/STUDENT_ASSIGNMENT.md

Zadání pro studenty bylo uloženo do STUDENT_ASSIGNMENT.md.

Git workflow: Commitnuto na větev docs/student-assignment (bb2f854), sloučeno (--no-ff) do develop (5fc8adf) a následně do main (55da716).
Stav repozitáře: main je čistá (nothing to commit, working tree clean).
Poznámka před rozdáním studentům: V sekci 2. Obtaining the Dataset stačí doplnit konkrétní <DATASET_DOWNLOAD_URL> nebo <COURSE_BUCKET> + <S3_OR_MINIO_ENDPOINT_URL> podle toho, kde jim předgenerovaný dataset nasdílíš.
Jakou by si navrhoval flow nad tím zadáním. Co bych měl studentům ještě poskytnout, kam jim nasdílet data atp. 

Tady je praktický návrh kompletní výukové flow, distribuce dat i toho, co studentům dát jako starter kit (a co jim naopak schovat), aby řešili skutečný data engineering a nezasekli se 2 týdny na rozbíhání Dockeru a JAR knihoven.

1. Doporučená výuková / projektová flow (milníky)

Nejlépe funguje iterativní flow rozdělená do 5 checkpointů (ať už na semestr, nebo na intenzivní blok):

Fáze 0 — Setup & Data Discovery (Týden 1):
Studenti naklonují tvůj Starter Repo (funkční docker-compose), stáhnou předgenerovaný dataset a v notebooku udělají rychlý data profiling.
Výstup: Seznam všech nalezených DQ problémů v datech (sami musí objevit víkendové díry v fx_rates, EU data v invoices, VIP_CORP_001 skew, zombie timesheety i to, že záporné faktury jsou dobropisy, ne chyba).
Fáze 1 — Bronze & Základní Airflow DAG (Týden 2–3):
Napíšou parametrizovaný PySpark Bronze ingest (--batch-date YYYY-MM-DD) a zapojí ho do Airflow.
Checkpoint: Opakované spuštění stejného dne (2026-01-05) nesmí vytvořit duplicity.
Fáze 2 — Silver, Quarantine, SCD2 & Skew (Týden 4–6):
Přidají DQ vrstvu s karanténou (silver_quarantine.*), dq_audit_results, SCD Type 2 nad employees, forward-fill fx_rates a vyřeší skew join u crm_events.
Checkpoint: Screenshot ze Spark UI (před vs. po optimalizaci skew u VIP_CORP_001) + validace SCD2 historie.
Fáze 3 — Gold Marts & Backfill (Týden 7–8):
Postaví 5 Gold tabulek (včetně point-in-time mzdového nákladu ze SCD2 a gold_employee_churn_features) a pustí backfill přes celé období.
Fáze 4 — Reporting + „Produkční“ Holdout test u obhajoby (Týden 9–10):
Postaví jednoduchý dashboard (Streamlit / Superset / Metabase) nad Gold + dq_audit_results.
Skvělý trik na obhajobu: Studentům dej na vývoj jen prvních 75 dní (2026-01-01 – 2026-03-16) a posledních 15 dní (2026-03-17 – 2026-03-31) uvolni až před odevzdáním / na obhajobě jako „nový inkrementální drop“. Hned uvidíš, jestli jim opravdu funguje orchestrace, SCD2, FX forward-fill i idempotence bez ručního hackování.
2. Kam a jak studentům nasdílet data

90denní medium dataset má zhruba 667k řádků (po zazipování cca desítky MB, velmi kompaktní). Výběr úložiště záleží na tom, jak moc chceš simulovat cloud:

Varianta	Jak na to	Pro koho je nejlepší
1. GitHub Releases (nejjednodušší a zdarma)	Vytvoř veřejný/interní repo jen pro zadání a nahraj landing_days_01_75.zip + landing_days_76_90.zip jako Release assets (limit je 2 GB/soubor). Studenti stahují jedním curl -L.	Doporučeno jako default — nulové náklady, žádné přihlašování, funguje na každé síti, přímý link do skriptu.
2. Cloud Object Storage (Cloudflare R2 / AWS S3 / GCS — public read-only bucket)	Nahraj přímo přes corp-datagen --s3-bucket ... v Hive struktuře landing/.... Studenti si data buď stáhnou (aws s3 sync --no-sign-request), nebo je Spark čte rovnou přes s3a://.	Nejbližší praxi, ideální pokud učíš i práci se S3 / MinIO / s3a://. (Cloudflare R2 má 10 GB a egress zdarma.)
3. Školní LMS (Moodle / Teams / SharePoint / vlastní MinIO server)	ZIP archiv v Moodle nebo read-only bucket na fakultním MinIO.	Pokud data nesmí ven mimo univerzitní síť. (Pozor: Moodle/SharePoint linky často vyžadují přihlášení a nejdou stáhnout přes prostý curl.)

Praktický tip: Zabal do archivu i soubor MANIFEST.sha256 a rozděl archiv na:

landing_dev_days_01_75.zip (pro vývoj)
landing_holdout_days_76_90.zip (vydáš těsně před odevzdáním pro ověření inkrementálního běhu)
(volitelně vygeneruj eNPS texty předem přes --llm-provider gemini nebo ollama s --llm-pool-size 250, ať mají studenti v employee_surveys.jsonl skutečně pestrý LLM text pro NLP bonus!)
3. Co dalšího studentům poskytnout (a co schovat)
Co jim určitě dej (Starter Kit)
Starter repozitář s funkčním docker-compose.yml:
Největší zabiják času v kurzech Spark + Airflow je rozchození kompatibilních verzí (PySpark + delta-spark JARy + hadoop-aws + Airflow + Java 17 na Windows/WSL2/Apple Silicon).
Připrav jim hotový kostrový docker-compose.yml (např. Airflow + PySpark + Delta Lake + Streamlit/DuckDB, případně lokální MinIO), kde docker compose up naběhne do 2 minut a rovnou spustí 1 dummy Spark job.
1 referenční mini-pipeline (např. jen tabulka clients):
Ukázkový bronze_clients.py + silver_clients.py + 1 jednoduchý PySpark pytest test. Studenti okamžitě pochopí očekávanou strukturu kódu, konvenci partitioningu a logování, aniž bys jim prozradil řešení těžkých částí (SCD2, skew, FX LOCF).
Skript na stažení & verifikaci dat (scripts/download_data.sh + .ps1):
Ať všichni mají identickou cestu ./data/landing/... a nemusí řešit rozdíly mezi Windows a macOS/Linux.
„Sanity Check“ kontrolní čísla pro prvních 7–14 dní:
Krátká tabulka v docs/SANITY_CHECKS.md s orientačními hodnotami jen za první 2 týdny (např. "po očištění invoices za 2026-01-01..2026-01-14 byste měli mít cca X validních faktur, Y dobropisů a Z v karanténě"). Zabrání to masovým nedorozuměním v zadání.
Přesný glosář KPI vzorců (1 stránka):
Explicitní definice metrik, aby nevznikaly dohady u obhajoby:
eNPS = (% Promoters [9–10]) − (% Detractors [0–6]) (škála -100 až +100)
gross_margin_eur = net_revenue_eur − labor_cost_eur
compa_ratio = hourly_rate_eur / median(hourly_rate_eur | department, seniority_level)
Tie-breaker pravidlo pro near-duplicity (nebo jasná instrukce, že si ho mají zvolit a zdokumentovat).
Co studentům NEPOSKYTOVAT
Zdrojový kód tohoto repozitáře corp-datagen (zvlášť corruptor.py a hr.py):
Kdyby viděli corruptor.py, nemuseli by data profilovat a hledat chyby sami.
Kdyby viděli hr.py, znali by přesný vzorec churn modelu pro ML bonus.
Drž si data_generator_company jako svůj soukromý učitelský repozitář a studentům publikuj oddělený Course Starter Repo (se STUDENT_ASSIGNMENT.md, docker-compose.yml, skriptem na stažení dat a sanity checky).
4. Rychlý checklist před startem kurzu
Vygenerovat finální data s --seed 42 (případně s --llm-provider gemini / ollama pro bohatší texty eNPS).
Rozdělit vygenerovaný ./data/landing na dev (dny 1–75) a holdout (dny 76–90) + vygenerovat SHA256 kontrolní součty.
Nahrát archivy na GitHub Releases / R2 / S3 a doplnit URL do STUDENT_ASSIGNMENT.md.
Připravit lehký Student Starter Repo (docker-compose.yml, referenční clients job, download_data skript, SANITY_CHECKS.md).

Pokud chceš, můžeme jako další krok rovnou připravit buď exportní skript na rozdělení datasetu (dev + holdout ZIP + SHA256), nebo vygenerovat kostru Student Starter Repa (docker-compose.yml pro Spark + Airflow + Delta + Streamlit).

vygeneruješ mi to do separátního dokumentu?

Dokument byl uložen do INSTRUCTOR_COURSE_FLOW.md.

Git workflow: Commitnuto na větev docs/instructor-course-flow (26d6b47), sloučeno (--no-ff) do develop (a33f326) a následně do main (92f3ea3).
Stav repozitáře: main je čistá (nothing to commit, working tree clean).
Obsah dokumentu: Výuková flow po milnících (Fáze 0–4), rozdělení dat na dev (dny 1–75) a holdout (dny 76–90), možnosti sdílení dat (GitHub Releases, Cloudflare R2 / S3, školní LMS), obsah Student Starter Repa vs. privátní učitelské části, protokol obhajoby a předkurzový checklist.
jsme teď na folderu který má na sebe navázaný git. kdybych chtěl po tobě připravit starter repo, dokážeš změnit folderu, založit v nad ní nové git repo zatim private později public. a přesunout práci tam do nové session?

Přímo a na rovinu — tady je přesně to, co zvládnu a co musíš kliknout ty:

Co umím vs. co neumím
Přepnout aktivní složku v IDE nebo založit novou Antigravity session neumím — každá session je svázaná s otevřeným workspace (data_generator_company), takže přechod do nové složky a spuštění nové session musíš udělat v rozhraní ty.
Vytvořit privátní repo přímo na GitHubu z terminálu teď nejde — na stroji není nainstalované GitHub CLI (gh), takže samotné prázdné Private repo založíš za 15 sekund v prohlížeči na GitHubu (a před začátkem kurzu ho jedním klikem přepneš v Settings → Danger Zone → Change visibility → Public).
Lokální vedlejší složku (např. c:\Users\proch\Documents\python_projects\priprava vyuky 2026\spark_lakehouse_starter), v ní git init -b main a zkopírování STUDENT_ASSIGNMENT.md do ní vytvořit umím (s tvým schválením mimo sandbox).
Nejčistší postup krok za krokem
Na GitHubu založ nové Private repo (např. spark-lakehouse-starter, prázdné) a naklonuj ho vedle současného projektu:
powershell
cd "c:\Users\proch\Documents\python_projects\priprava vyuky 2026"
git clone https://github.com/smoothgrinder/spark-lakehouse-starter.git
(Případně tu novou složku můžeme jen lokálně inicializovat přes git init a GitHub remote připojit později.)
Zkopíruj do něj jen zadání (ne generátor!) — konkrétně docs/STUDENT_ASSIGNMENT.md.
Otevři spark-lakehouse-starter v Antigravity (nové okno / Open Folder) a spusť tam novou session.
Předání kontextu: Jakmile v nové session odkážeš na docs/STUDENT_ASSIGNMENT.md (nebo vložíš krátký handoff prompt, který ti tu můžu připravit), nová session má 100 % přesnou specifikaci všech 10 tabulek, schémat i architektury bez rizika, že se omylem smíchá kód učitelského generátoru se studentským repem.
Praktický tip na pořadí kroků

Než tuhle session v data_generator_company zavřeš, dává velký smysl nejdřív udělat ještě jednu věc přímo tady (protože tady je generátor):

Vygenerovat finální data (--seed 42) a připravit malý balíčkovací skript, který vyrobí landing_dev_days_01_75.zip, landing_holdout_days_76_90.zip, MANIFEST.sha256 a automaticky spočítá přesná čísla do SANITY_CHECKS.md (za prvních 14 dní).
A pak se čistě přepnout do nového privátního spark-lakehouse-starter repa a postavit tam docker-compose.yml + referenční clients pipeline + download skripty.

Chceš nejdřív tady udělat ten exportní balíčkovač + SANITY_CHECKS.md, nebo mám rovnou připravit lokální vedlejší složku spark-lakehouse-starter a handoff prompt pro novou session?

# Course Project Assignment: Enterprise Lakehouse & Medallion Pipeline with Apache Spark

## 1. Scenario & Objective

You are joining the Data Engineering team of a mid-sized consulting and technology company (**CorpTech Consulting**). Each day, operational source systems across **HR**, **Finance**, **Business/CRM**, and **People Analytics (eNPS)** export raw batch files into a shared **Landing Zone** (`./data/landing`).

Your objective is to design, implement, orchestrate, and test an end-to-end **Apache Spark Lakehouse** following the **Medallion Architecture (`Bronze → Silver → Gold`)**, backed by automated **Data Quality (DQ) checks**, **workflow orchestration**, and a **lightweight reporting layer** built on top of the Gold layer.

By the end of the project, your platform must:
1. **Acquire & ingest** pre-generated daily partitioned and full-load raw files reliably into a **Bronze** layer.
2. **Clean, validate, conform, and enrich** the data in a **Silver** layer (including **SCD Type 2**, quarantine tables, currency normalization, and Spark shuffle-skew mitigation).
3. **Model business-ready Gold data marts** (financial profitability, HR & eNPS analytics, CRM engagement, and an ML-ready churn feature table).
4. **Orchestrate** daily incremental runs and historical backfills with automated **Data Quality gates**.
5. **Visualize** key business KPIs and pipeline DQ health in a **simple Reporting Layer**.

---

## 2. Obtaining the Dataset

A pre-generated 90-day corporate dataset (`2026-01-01` to `2026-03-31`, ~667,000 rows across 10 datasets) is provided for this course. You must download it to your local or containerized landing zone (`./data/landing`) before running your pipelines.

> **Instructor Configuration:** Replace the placeholder below with your course distribution method (LMS archive link, HTTP mirror, or S3/MinIO bucket).

### Option A — Download & Extract Archive (HTTP / LMS)
```bash
mkdir -p ./data/landing
curl -L "<DATASET_DOWNLOAD_URL>" -o ./data/landing_dataset.zip
unzip ./data/landing_dataset.zip -d ./data/landing

```

### Option B — Sync from Course S3 / MinIO Bucket
```bash
aws s3 sync s3://<COURSE_BUCKET>/landing ./data/landing \
  --endpoint-url "<S3_OR_MINIO_ENDPOINT_URL>"

```

### Verify Your Download
After downloading, confirm that `./data/landing` contains all 10 source folders (`clients`, `projects`, `survey_questions`, `employees`, `timesheets`, `fx_rates`, `invoices`, `expenses`, `crm_events`, `employee_surveys`):

```bash
ls -la ./data/landing

```

- **Do not commit the downloaded `./data/` folder to Git** — include `data/` in your `.gitignore` and provide a script/Make target or Airflow ingestion task that pulls or reads from the configured landing path.
- For rapid local iteration and unit tests, you may process a shorter date slice (e.g., `2026-01-01` to `2026-01-14`), but your final pipeline and reporting layer must be validated on the full **90-day** dataset.

---

## 3. Data Description (Landing Zone)

The Landing Zone contains full-load reference datasets alongside daily batch drops organized using Hive-style directory partitioning (`date=YYYY-MM-DD/part-000.<ext>`):

```text
data/landing/
├── clients/clients.csv
├── projects/projects.csv
├── survey_questions/survey_questions.json
├── employees/date=YYYY-MM-DD/part-000.csv
├── timesheets/date=YYYY-MM-DD/part-000.parquet
├── fx_rates/date=YYYY-MM-DD/part-000.json
├── invoices/date=YYYY-MM-DD/part-000.jsonl
├── expenses/date=YYYY-MM-DD/part-000.csv
├── crm_events/date=YYYY-MM-DD/part-000.jsonl
└── employee_surveys/date=YYYY-MM-DD/part-000.jsonl

```

### 3.1 Business / CRM Domain

| Dataset | Format & Load Type | Columns & Semantics |
Gemini 4 Carbon now available!

Both Carbon and Argon are now included as part of the entire Gemini entitlement, so you can use them for much longer.
