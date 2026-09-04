"""AST-derived read-only evidence; unsupported shapes explicitly remain inconclusive."""

import sqlglot
from sqlglot import exp


def suspicious_result(result):
    tree = sqlglot.parse_one(result["sql"], read="sqlite")
    return not result["rows"] or (
        any(tree.find_all(exp.AggFunc))
        and any(v is None for row in result["rows"] for v in row.values())
    )


def temporal_aggregate(result, catalog):
    """Higher-risk date arithmetic warrants review even when it returns a number."""
    timestamps = {
        name.lower()
        for entity in catalog.get("entities", {}).values()
        for name in entity.get("timestamps", {})
    }
    tree = sqlglot.parse_one(result["sql"], read="sqlite")
    return any(
        any(c.name.lower() in timestamps for c in agg.find_all(exp.Column))
        for agg in tree.find_all(exp.AggFunc)
    )


def duration_policy_errors(sql, catalog):
    """Structural guard for catalog temporal AVG; not a proof of natural-language intent."""
    timestamps = {
        name.lower()
        for e in catalog.get("entities", {}).values()
        for name in e.get("timestamps", {})
    }
    tree = sqlglot.parse_one(sql, read="sqlite")
    errors = []
    for avg in tree.find_all(exp.Avg):
        if not any(c.name.lower() in timestamps for c in avg.find_all(exp.Column)):
            continue
        if not any(avg.find_all(exp.Case)):
            errors.append(
                "Temporal AVG must use CASE inside AVG to exclude missing, unparseable and negative durations, retaining zero. Do not average raw durations; return matched/valid/excluded counts."
            )
    return errors


def diagnostic_count_errors(result):
    """Check the duration-count contract against independent input diagnostics."""
    counts = result.get("diagnostics", {}).get("counts", {})
    if "duration_0_valid_count" not in counts or "duration_1_valid_count" in counts:
        return []
    if len(result["rows"]) != 1:
        return []
    row = result["rows"][0]
    mapping = {
        "matched_count": "matched_count",
        "valid_duration_count": "duration_0_valid_count",
        "excluded_duration_count": "duration_0_excluded_count",
    }
    return [
        f"Output {column}={row[column]!r} contradicts diagnostic {counts[key]}. Preserve filters and fix count expression; excluded = matched - valid."
        for column, key in mapping.items()
        if column in row and row[column] != counts[key]
    ]


def render_diagnostics(report):
    """Causal explanations use measured counts only, never model guesses."""
    counts = report.get("counts")
    if counts is None:
        return "Chẩn đoán chưa đủ để xác định nguyên nhân; không suy ra dữ liệu nguồn bị thiếu."
    if counts["matched_count"] == 0:
        return "Không có hàng đầu vào khớp bộ lọc SQL đã chạy; chưa thể kết luận dữ liệu nguồn không được ghi nhận."
    lines = [f"Có {counts['matched_count']} hàng đầu vào khớp bộ lọc SQL đã chạy."]
    for index in range(4):
        prefix = f"duration_{index}_"
        if prefix + "valid_count" not in counts:
            continue
        lines.append(
            f"Khoảng thời gian #{index + 1}: {counts[prefix + 'valid_count']} hợp lệ; "
            f"{counts[prefix + 'missing_count']} thiếu timestamp; "
            f"{counts[prefix + 'unparseable_count']} không chuyển đổi được timestamp; "
            f"{counts[prefix + 'negative_count']} có thời gian âm."
        )
    lines.append(
        "Các số đếm mô tả phạm vi SQL đã chạy, không chứng minh SQL đã hiểu đúng toàn bộ câu hỏi."
    )
    return "\n".join(lines)


def diagnose(snapshot, result):
    """Keep FROM/JOIN/WHERE intact; never claim pre-filter data is missing."""
    tree = sqlglot.parse_one(snapshot.validate(result["sql"]), read="sqlite")
    report = {
        "status": "inconclusive",
        "queries": [],
        "observations": [],
        "scope": "Diagnostics describe the executed SQL, not proof it matches user intent.",
    }
    if not isinstance(tree, exp.Select) or tree.args.get("group") or tree.args.get("having"):
        report["reason"] = "Grouped/set results need per-group diagnosis; no global cause inferred."
        return report
    if any(tree.find_all(exp.Window)) or any(tree.find_all(exp.Subquery)):
        report["reason"] = "Window/subquery shape requires scoped diagnosis; no cause inferred."
        return report
    selections = ["COUNT(*) AS matched_count"]
    for index, agg in enumerate(tree.find_all(exp.AggFunc)):
        if index >= 8:
            report["reason"] = "Aggregate diagnostic limit exceeded."
            return report
        if agg.this is not None and not isinstance(agg.this, exp.Star):
            selections.append(
                f"COUNT({agg.this.sql(dialect='sqlite')}) AS aggregate_{index}_nonnull_inputs"
            )
    duration_index = 0
    seen_durations = set()
    for sub in tree.find_all(exp.Sub):
        end, start = sub.this, sub.expression
        if not all(
            isinstance(n, exp.Anonymous)
            and n.name.lower() == "julianday"
            and len(n.expressions) == 1
            for n in (end, start)
        ):
            continue
        signature = sub.sql(dialect="sqlite")
        if signature in seen_durations:
            continue
        seen_durations.add(signature)
        if duration_index >= 4:
            report["reason"] = "Duration diagnostic limit exceeded."
            return report
        prefix = f"duration_{duration_index}"
        raw_end, raw_start = (n.expressions[0].sql(dialect="sqlite") for n in (end, start))
        parsed_end, parsed_start = (n.sql(dialect="sqlite") for n in (end, start))
        raw = f"{raw_end} IS NOT NULL AND {raw_start} IS NOT NULL"
        parsed = f"{parsed_end} IS NOT NULL AND {parsed_start} IS NOT NULL"
        valid = f"{parsed} AND {parsed_end} >= {parsed_start}"
        for suffix, condition in (
            ("nonnull_pairs", raw),
            ("parseable_pairs", parsed),
            ("valid_count", valid),
            ("negative_count", f"{parsed} AND {parsed_end} < {parsed_start}"),
            ("missing_count", f"NOT ({raw})"),
            ("unparseable_count", f"({raw}) AND NOT ({parsed})"),
            ("excluded_count", f"NOT ({valid})"),
        ):
            selections.append(f"COUNT(CASE WHEN {condition} THEN 1 END) AS {prefix}_{suffix}")
        duration_index += 1
    diagnostic = tree.copy()
    for key in ("order", "limit", "offset", "distinct"):
        diagnostic.set(key, None)
    diagnostic.set("expressions", [sqlglot.parse_one(s, read="sqlite") for s in selections])
    evidence = snapshot.execute(diagnostic.sql(dialect="sqlite"))
    report["queries"].append(evidence)
    if evidence["truncated"] or len(evidence["rows"]) != 1:
        report["reason"] = "Diagnostic evidence was limited; no cause inferred."
        return report
    counts = evidence["rows"][0]
    report["status"] = "observed"
    report["counts"] = counts
    if counts["matched_count"] == 0:
        report["observations"].append(
            "No input rows match the executed SQL filters; this does not prove source data is absent."
        )
    else:
        report["observations"].append(
            "Input rows match the executed SQL; inspect non-NULL and duration counts before explaining missing aggregates."
        )
    return report
