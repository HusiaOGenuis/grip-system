@app.post("/report")
def report(
    dataset=Depends(get_dataset),
    api_key: str = Depends(get_current_user)
):

    # -----------------------------
    # PAYMENT CONTRACT
    # -----------------------------
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT is_paid FROM users WHERE api_key=%s",
                (api_key,)
            )
            row = cur.fetchone()

            if not row or row[0] is not True:
                raise HTTPException(
                    status_code=402,
                    detail="PAYMENT_REQUIRED"
                )

    # -----------------------------
    # ANALYSIS + SCORING
    # -----------------------------
    analysis = analyze_data(dataset["data"])
    score = grip_score(dataset["data"])

    # -----------------------------
    # PDF GENERATION
    # -----------------------------
    file_path = generate_pdf(
        dataset["id"],
        analysis,
        score
    )

    # -----------------------------
    # STORE REPORT (CRITICAL FIX)
    # -----------------------------
    report_id = str(uuid.uuid4())

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reports (id, dataset_id, api_key, report_json)
                VALUES (%s, %s, %s, %s)
            """, (
                report_id,
                dataset["id"],
                api_key,
                Json({
                    "analysis": analysis,
                    "score": score,
                    "file": file_path
                })
            ))

    # -----------------------------
    # RESPONSE
    # -----------------------------
    return {
        "report_id": report_id,
        "dataset_id": dataset["id"],
        "analysis": analysis,
        "score": score,
        "report_file": file_path
    }