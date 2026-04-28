@app.post("/report")
def report(
    dataset=Depends(get_dataset),
    api_key: str = Depends(get_current_user)
):
    try:
        # -----------------------------
        # PAYMENT CHECK
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
        # ANALYSIS
        # -----------------------------
        data = dataset.get("data")
        if not data:
            raise HTTPException(400, "INVALID_DATASET")

        analysis = analyze_data(data)
        score = grip_score(data)

        # -----------------------------
        # PDF GENERATION (SAFE)
        # -----------------------------
        file_path = generate_pdf(dataset["id"], analysis, score)

        if not file_path:
            raise Exception("PDF_GENERATION_FAILED")

        # -----------------------------
        # STORE REPORT
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

        return {
            "report_id": report_id,
            "analysis": analysis,
            "score": score,
            "report_file": file_path
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"REPORT_FAILED: {str(e)}"
        )