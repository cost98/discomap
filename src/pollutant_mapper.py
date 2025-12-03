"""
EEA Pollutant Code Mapper

Maps EEA numeric pollutant codes to human-readable names.
Based on: https://dd.eionet.europa.eu/vocabulary/aq/pollutant
"""

# EEA Pollutant Mapping (most common pollutants)
POLLUTANT_CODES = {
    1: "SO2",  # Sulphur dioxide
    5: "PM10",  # Particulate matter < 10 µm
    6001: "PM2.5",  # Particulate matter < 2.5 µm
    7: "O3",  # Ozone
    8: "NO2",  # Nitrogen dioxide
    9: "NOX",  # Nitrogen oxides as NO2
    10: "CO",  # Carbon monoxide
    20: "C6H6",  # Benzene
    5012: "PM10",  # PM10 (aerosol)
    5029: "PM2.5",  # PM2.5 (aerosol)
    12: "Pb",  # Lead (aerosol)
    14: "Cd",  # Cadmium (aerosol)
    15: "Ni",  # Nickel (aerosol)
    18: "As",  # Arsenic (aerosol)
    21: "C7H8",  # Toluene
    5610: "BaP",  # Benzo(a)pyrene
}

# Reverse mapping (name to code)
POLLUTANT_NAMES = {v: k for k, v in POLLUTANT_CODES.items()}


def get_pollutant_name(code):
    """Convert numeric code to pollutant name."""
    return POLLUTANT_CODES.get(int(code), f"Unknown_{code}")


def get_pollutant_code(name):
    """Convert pollutant name to numeric code."""
    return POLLUTANT_NAMES.get(name.upper())


def add_pollutant_names(df):
    """Add pollutant name column to dataframe."""
    import pandas as pd

    df = df.copy()
    df["PollutantName"] = df["Pollutant"].apply(get_pollutant_name)
    return df


if __name__ == "__main__":
    print("EEA Pollutant Code Mapping")
    print("=" * 60)
    for code, name in sorted(POLLUTANT_CODES.items()):
        print(f"{code:4d} -> {name}")
