from src.transform.runner import render_sql


def test_render_sql():
    sql = "SELECT '{{ run_date }}' AS dt"
    rendered = render_sql(sql, "2025-01-01")
    assert "2025-01-01" in rendered