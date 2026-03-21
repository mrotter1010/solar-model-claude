"""End-to-end test: full pipeline with bill calculation enabled."""

import csv
import json
import shutil
import tempfile
from pathlib import Path

import pytest

from src.pipeline import SolarModelingPipeline

FIXTURE_CSV = Path("tests/fixtures/rates/bill_test_input.csv")
RESULTS_DIR = Path("tests/test_results/end_to_end_bill")


@pytest.fixture(scope="module")
def pipeline_result():
    """Run the full pipeline once for all tests in this module.

    Returns the result dict from SolarModelingPipeline.run().
    """
    output_dir = Path(tempfile.mkdtemp(prefix="bill_e2e_"))
    pipeline = SolarModelingPipeline(output_dir=output_dir)
    result = pipeline.run(csv_path=FIXTURE_CSV)
    yield result
    # Cleanup is optional — leave temp dir for inspection if needed


class TestBillCalculationEndToEnd:
    """Full pipeline run with bill_calculation=True."""

    def test_pipeline_succeeded(self, pipeline_result: dict) -> None:
        """Pipeline should complete with at least 1 successful site."""
        assert pipeline_result["successful"] >= 1, (
            f"Expected at least 1 success, got {pipeline_result['successful']}. "
            f"Failed: {pipeline_result['failed']}"
        )

    def test_bill_savings_key_present(self, pipeline_result: dict) -> None:
        """Summary dict should contain 'bill_savings' key."""
        summaries = pipeline_result["summaries"]
        assert len(summaries) >= 1, "No summaries returned"
        summary = summaries[0]
        assert "bill_savings" in summary, (
            f"Missing 'bill_savings' key in summary. Keys: {list(summary.keys())}"
        )

    def test_bill_savings_structure(self, pipeline_result: dict) -> None:
        """bill_savings should contain all expected fields."""
        bill_savings = pipeline_result["summaries"][0]["bill_savings"]

        required_keys = [
            "annual_bill_without_solar",
            "annual_bill_with_solar",
            "annual_savings",
            "savings_percent",
            "avoided_cost_per_kwh",
            "monthly_detail",
        ]
        for key in required_keys:
            assert key in bill_savings, f"Missing key: {key}"

        # monthly_detail should have 12 entries
        assert len(bill_savings["monthly_detail"]) == 12, (
            f"Expected 12 monthly entries, got {len(bill_savings['monthly_detail'])}"
        )

    def test_annual_savings_positive(self, pipeline_result: dict) -> None:
        """Solar should produce positive savings."""
        bill_savings = pipeline_result["summaries"][0]["bill_savings"]
        assert bill_savings["annual_savings"] > 0, (
            f"Expected positive savings, got {bill_savings['annual_savings']}"
        )

    def test_savings_percent_valid(self, pipeline_result: dict) -> None:
        """Savings percent should be > 0 and < 100."""
        bill_savings = pipeline_result["summaries"][0]["bill_savings"]
        assert 0 < bill_savings["savings_percent"] < 100, (
            f"Savings percent out of range: {bill_savings['savings_percent']}"
        )

    def test_avoided_cost_positive(self, pipeline_result: dict) -> None:
        """Avoided cost per kWh should be positive."""
        bill_savings = pipeline_result["summaries"][0]["bill_savings"]
        assert bill_savings["avoided_cost_per_kwh"] > 0, (
            f"Expected positive avoided cost, got {bill_savings['avoided_cost_per_kwh']}"
        )

    def test_print_results(self, pipeline_result: dict) -> None:
        """Print key results for manual inspection and write to test_results."""
        summary = pipeline_result["summaries"][0]
        bill_savings = summary["bill_savings"]

        # Print key results
        annual_production_kwh = summary["annual_energy_mwh"] * 1000
        print("\n" + "=" * 60)
        print("END-TO-END BILL CALCULATION RESULTS")
        print("=" * 60)
        print(f"Annual Production:       {annual_production_kwh:,.0f} kWh")
        print(f"Annual Consumption:      2,000,000 kWh")
        print(f"Bill WITHOUT Solar:      ${bill_savings['annual_bill_without_solar']:,.2f}")
        print(f"Bill WITH Solar:         ${bill_savings['annual_bill_with_solar']:,.2f}")
        print(f"Annual Savings:          ${bill_savings['annual_savings']:,.2f}")
        print(f"Savings %:               {bill_savings['savings_percent']:.1f}%")
        print(f"Avoided Cost ($/kWh):    ${bill_savings['avoided_cost_per_kwh']:.4f}")
        print("-" * 60)
        print(f"{'Month':<10} {'Without':>12} {'With':>12} {'Savings':>12}")
        print("-" * 60)
        for m in bill_savings["monthly_detail"]:
            print(
                f"{m['month']:<10} ${m['bill_without']:>10,.2f} "
                f"${m['bill_with']:>10,.2f} ${m['savings']:>10,.2f}"
            )
        print("=" * 60)

        # Write results to test_results directory
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)

        # bill_summary.json
        summary_path = RESULTS_DIR / "bill_summary.json"
        summary_path.write_text(json.dumps(bill_savings, indent=2, default=str))

        # monthly_comparison.csv
        csv_path = RESULTS_DIR / "monthly_comparison.csv"
        with csv_path.open("w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["month", "bill_without", "bill_with", "savings"]
            )
            writer.writeheader()
            for m in bill_savings["monthly_detail"]:
                writer.writerow(m)

        print(f"\nResults written to {RESULTS_DIR}/")


class TestBillCalculationDisabled:
    """Pipeline run with bill_calculation=FALSE should not produce bill_savings."""

    def test_no_bill_savings_when_disabled(self, tmp_path: Path) -> None:
        """When Bill Calculation is FALSE, summary should not contain bill_savings."""
        # Create a modified CSV with Bill Calculation = FALSE
        modified_csv = tmp_path / "bill_disabled.csv"
        original = FIXTURE_CSV.read_text()
        # Replace TRUE with FALSE in the Bill Calculation column
        lines = original.strip().split("\n")
        header = lines[0]
        data = lines[1]

        # Find the Bill Calculation column index
        headers = header.split(",")
        bill_col_idx = headers.index("Bill Calculation")

        # Split data and set Bill Calculation to FALSE
        data_parts = data.split(",")
        data_parts[bill_col_idx] = "FALSE"
        modified_data = ",".join(data_parts)

        modified_csv.write_text(header + "\n" + modified_data + "\n")

        # Run pipeline
        output_dir = tmp_path / "output"
        pipeline = SolarModelingPipeline(output_dir=output_dir)
        result = pipeline.run(csv_path=modified_csv)

        assert result["successful"] >= 1, (
            f"Pipeline should succeed, got {result['successful']} successes"
        )

        summary = result["summaries"][0]
        assert "bill_savings" not in summary, (
            "bill_savings should NOT be present when Bill Calculation is FALSE"
        )
