"""
Generate a self-contained set of dummy data files for the Provider Network
Comparison assignment.

Running this script creates a `sample_data/` tree next to it that mirrors the
directory layout and column schemas the two assignment scripts expect:

    sample_data/
      SAMPLE/
        network_files/
          Individual/<network_id>/<specialty_code>.parquet
          Organization/<network_id>.csv
        Score for Individual - PNC Medicare.csv
        Scores for Organization - PNC Medicare.csv
        HSD_specialty.csv
        network_performance_countywise.csv
        network_performance_countywise_hospital.csv
        Hospital_name_mapping.csv
      output/
        Individual/
        Organization/

All values are randomly generated and reference no real providers or
organizations. Run with:  python generate_dummy_data.py
"""

import os
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# Scenario configuration (matches the example arguments in the two scripts)
# ----------------------------------------------------------------------------
SEED = 42
FILESPATH = "SAMPLE"
PLAN_TYPE_ID = "3"
PLAN_TYPE = "HMO/HMO-POS"

# network_id (folder/file name)  ->  (parent_org_id, parent_org_name)
NETWORKS = {
    100001: ("1", "Org A"),
    100002: ("2", "Org B"),
}

# FIPS State County Code -> (state, county_name)
COUNTIES = {
    6037: ("CA", "Los Angeles"),
    4013: ("AZ", "Maricopa"),
}

IND_FLAGS = ["PCP", "Physician Specialists", "Other Providers"]
SPECIALIST_SPECIALTIES = ["Cardiology", "Orthopedic Surgery", "Dermatology"]
OTHER_SPECIALTIES = ["Nurse Practitioner", "Physician Assistant"]
PCP_SPECIALTY = "Family Medicine"
SCORING_CATEGORIES = ["High", "Medium", "Low"]

# Specialty codes used as parquet file names. These are intentionally NOT in the
# `spec_code` exclusion list inside the scripts, so the files are actually read.
FLAG_TO_FILE_CODE = {
    "PCP": "90001",
    "Physician Specialists": "90002",
    "Other Providers": "90003",
}

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_data")
SAMPLE_DIR = os.path.join(ROOT, FILESPATH)

rng = np.random.default_rng(SEED)


def _ensure_dirs():
    for sub in [
        os.path.join(SAMPLE_DIR, "network_files", "Individual"),
        os.path.join(SAMPLE_DIR, "network_files", "Organization"),
        os.path.join(ROOT, "output", "Individual"),
        os.path.join(ROOT, "output", "Organization"),
    ]:
        os.makedirs(sub, exist_ok=True)


def _npi_pool():
    """Build a shared NPI pool so some providers overlap between networks
    (producing 'common' rows) while others are unique to one network."""
    base = 1000000000
    return {
        "pcp": list(range(base, base + 12)),
        "spec": list(range(base + 100, base + 112)),
        "other": list(range(base + 200, base + 212)),
        "hospital": list(range(base + 900, base + 912)),
    }


POOL = _npi_pool()


def _individual_rows(network_id):
    org_id, org_name = NETWORKS[network_id]
    network_name = f"{org_name} {PLAN_TYPE}"
    rows = []

    def add(flag, npi_list, specialties):
        for fips, (state, county_name) in COUNTIES.items():
            for npi in npi_list:
                spec_desc = str(rng.choice(specialties))
                rows.append({
                    "npi": npi,
                    "type": "Individual",
                    "presentation_name": f"Provider {npi}",
                    "first_name": f"First{npi % 1000}",
                    "last_name": f"Last{npi % 1000}",
                    "network_id": network_id,
                    "network_name": network_name,
                    "Address First Line": f"{npi % 9999} Main St",
                    "Parent_Organization": org_name,
                    "location_confidence": str(rng.choice(["Low", "Medium", "High"])),
                    "location_confidence_address_level": str(rng.choice(["Low", "Medium", "High"])),
                    "specialty_category": flag,
                    "specialty_name": spec_desc,
                    "specialty_subspecialty": spec_desc,
                    "specialty_code": FLAG_TO_FILE_CODE[flag],
                    "Specialty Flag": str(rng.choice(["Primary", "Secondary"])),
                    "New Flag": flag,
                    "city": county_name,
                    "state": state,
                    "zip_code": f"{10000 + (npi % 89999):05d}",
                    "county_name": county_name,
                    "FIPS State County Code": fips,
                    "Specialty Description": "PCP" if flag == "PCP" else spec_desc,
                    "Is_Dual_Provider": str(rng.choice(["Y", "N"])),
                    "Affiliation ID": 700000 + (npi % 5000),
                    "Affiliation Presentation Name": f"Group {npi % 50}",
                }) 

    # Each network gets a subset of the pool so overlap and uniqueness exist.
    if network_id == 100001:
        add("PCP", POOL["pcp"][:9], [PCP_SPECIALTY])
        add("Physician Specialists", POOL["spec"][:9], SPECIALIST_SPECIALTIES)
        add("Other Providers", POOL["other"][:9], OTHER_SPECIALTIES)
    else:
        add("PCP", POOL["pcp"][3:], [PCP_SPECIALTY])
        add("Physician Specialists", POOL["spec"][3:], SPECIALIST_SPECIALTIES)
        add("Other Providers", POOL["other"][3:], OTHER_SPECIALTIES)

    return pd.DataFrame(rows)


def _write_individual(network_id):
    df = _individual_rows(network_id)
    net_dir = os.path.join(SAMPLE_DIR, "network_files", "Individual", str(network_id))
    os.makedirs(net_dir, exist_ok=True)
    for flag, code in FLAG_TO_FILE_CODE.items():
        part = df[df["New Flag"] == flag]
        part.to_parquet(os.path.join(net_dir, f"{code}.parquet"), index=False)
    return df


def _organization_rows(network_id):
    org_id, org_name = NETWORKS[network_id]
    network_name = f"{org_name} {PLAN_TYPE}"
    npis = POOL["hospital"][:9] if network_id == 100001 else POOL["hospital"][3:]
    rows = []
    for fips, (state, county_name) in COUNTIES.items():
        for npi in npis:
            rows.append({
                "npi": npi,
                "type": "Organization",
                "presentation_name": f"hospital {npi % 100}",
                "first_name": "",
                "last_name": "",
                "network_id": network_id,
                "network_name": network_name,
                "carrier_name": org_name,
                "specialty_category": "Hospital",
                "specialty_name": "Hospital",
                "specialty_subspecialty": "Hospital",
                "specialty_code": "80001",
                "New Flag": "Hospital",
                "city": county_name,
                "state": state,
                "zip_code": f"{10000 + (npi % 89999):05d}",
                "county_name": county_name,
                "FIPS State County Code": fips,
                "Address First Line": f"{npi % 9999} Hospital Ave",
                "Status": "ACTIVE",
            })
    return pd.DataFrame(rows)


def _write_organization(network_id):
    df = _organization_rows(network_id)
    path = os.path.join(SAMPLE_DIR, "network_files", "Organization", f"{network_id}.csv")
    df.to_csv(path, index=False)
    return df


def _write_individual_scores(all_ind):
    """Generate scores for ~70% of individual NPIs so some merges hit and the
    rest fall back to 'Not Available' in the scripts."""
    base = all_ind[["npi", "New Flag", "FIPS State County Code", "Specialty Description"]].drop_duplicates()
    keep = base.sample(frac=0.7, random_state=SEED)
    rows = []
    for _, r in keep.iterrows():
        specialty = "PCP" if r["New Flag"] == "PCP" else r["Specialty Description"]
        rows.append({
            "NPI": int(r["npi"]),
            "New Flag": r["New Flag"],
            "County FIPS": int(r["FIPS State County Code"]),
            "Cost Score": round(float(rng.uniform(1, 5)), 2),
            "Quality Score": round(float(rng.uniform(1, 5)), 2),
            "Specialty": specialty,
            "NPI Score": round(float(rng.uniform(1, 5)), 2),
            "MA Utilization Score": round(float(rng.uniform(0, 100)), 2),
            "FFS Utilization Score": round(float(rng.uniform(0, 100)), 2),
            "Quality color indicator": str(rng.choice(["Green", "Yellow", "Red"])),
        })
    pd.DataFrame(rows).to_csv(
        os.path.join(SAMPLE_DIR, "Score for Individual - PNC Medicare.csv"), index=False)


def _write_org_scores(all_org):
    base = all_org[["npi", "Address First Line", "state", "FIPS State County Code"]].drop_duplicates()
    base["Address First Line"] = base["Address First Line"].str.title()
    keep = base.sample(frac=0.7, random_state=SEED)
    rows = []
    for _, r in keep.iterrows():
        rows.append({
            "npi": int(r["npi"]),
            "Address First Line": r["Address First Line"],
            "state": r["state"],
            "FIPS State County Code": int(r["FIPS State County Code"]),
            "Quality Score": round(float(rng.uniform(1, 5)), 2),
            "Cost Score": round(float(rng.uniform(1, 5)), 2),
            "FFS Utilization Score": round(float(rng.uniform(0, 100)), 2),
            "MA Utilization Score": round(float(rng.uniform(0, 100)), 2),
        })
    pd.DataFrame(rows).to_csv(
        os.path.join(SAMPLE_DIR, "Scores for Organization - PNC Medicare.csv"), index=False)


def _write_hsd_specialty():
    specialties = [PCP_SPECIALTY] + SPECIALIST_SPECIALTIES + OTHER_SPECIALTIES
    pd.DataFrame({"Specialty Description": specialties}).to_csv(
        os.path.join(SAMPLE_DIR, "HSD_specialty.csv"), index=False)


def _write_market_individual():
    rows = []
    for fips, (state, county_name) in COUNTIES.items():
        for flag in IND_FLAGS:
            for cat in SCORING_CATEGORIES:
                rows.append({
                    "state": state,
                    "county_name": county_name,
                    "FIPS State County Code": fips,
                    "New Flag": flag,
                    "Scoring Category": cat,
                    "Quality Score": int(rng.integers(1, 50)),
                    "Cost Score": int(rng.integers(1, 50)),
                    "MA Utilization Score": int(rng.integers(1, 50)),
                    "FFS Utilization Score": int(rng.integers(1, 50)),
                })
    pd.DataFrame(rows).to_csv(
        os.path.join(SAMPLE_DIR, "network_performance_countywise.csv"), index=False)


def _write_market_hospital():
    rows = []
    for fips, (state, county_name) in COUNTIES.items():
        for cat in SCORING_CATEGORIES:
            rows.append({
                "state": state,
                "county_name": county_name,
                "FIPS County Code": fips,
                "New Flag": "Hospital",
                "Scoring Category": cat,
                "Quality Score": int(rng.integers(1, 50)),
                "Cost Score": int(rng.integers(1, 50)),
                "MA Utilization Score": int(rng.integers(1, 50)),
                "FFS Utilization Score": int(rng.integers(1, 50)),
            })
    pd.DataFrame(rows).to_csv(
        os.path.join(SAMPLE_DIR, "network_performance_countywise_hospital.csv"), index=False)


def _write_hospital_mapping(all_org):
    names = sorted(all_org["presentation_name"].str.strip().str.upper().unique())
    rows = [{"Presentation Name": n, "AHD_Facility Name": n.title() + " Medical Center"} for n in names]
    pd.DataFrame(rows).to_csv(
        os.path.join(SAMPLE_DIR, "Hospital_name_mapping.csv"), index=False)


def main():
    _ensure_dirs()

    all_ind = []
    all_org = []
    for network_id in NETWORKS:
        all_ind.append(_write_individual(network_id))
        all_org.append(_write_organization(network_id))
    all_ind = pd.concat(all_ind, ignore_index=True)
    all_org = pd.concat(all_org, ignore_index=True)

    _write_individual_scores(all_ind)
    _write_org_scores(all_org)
    _write_hsd_specialty()
    _write_market_individual()
    _write_market_hospital()
    _write_hospital_mapping(all_org)

    print(f"Dummy data written under: {ROOT}")
    print(f"  Individual provider rows: {len(all_ind)}")
    print(f"  Organization (hospital) rows: {len(all_org)}")
    print("Example run arguments (counts script / download script):")
    print('  countyId   = "6037,4013"')
    print('  id_param   = "1"')
    print(f'  filespath  = "{FILESPATH}"')
    print('  clientID   = "45"')
    print('  userID     = "demo-user"')
    print('  base       = \'{"1": {"parentOrganization": "Org A", "planBidIds": "H0000_001_0", "networkIds": "100001"}}\'')
    print('  compare    = \'{"2": {"parentOrganization": "Org B", "planBidIds": "H0000_002_0", "networkIds": "100002"}}\'')
    print('  baseParentOrg="Org A"  compareParentOrg="Org B"')
    print('  baseParentOrgId="1"  compareParentOrgId="2"')
    print(f'  planType="{PLAN_TYPE}"  planTypeId="{PLAN_TYPE_ID}"')


if __name__ == "__main__":
    main()
