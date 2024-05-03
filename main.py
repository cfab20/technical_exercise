
import pandas as pd
import numpy as np
from datetime import datetime
from typing_extensions import Literal
import uuid


EPA_FILE = "EPA_ghg-emission-factors-hub.xlsx"
TARGET_COLS = [
    "sector", 
    "category", 
    "activity_id", 
    "name", 
    "activity_unit", 
    "kgCO2e-AR5", 
    "kgCO2e-AR4", 
    "kgCO2", 
    "kgCH4", 
    "kgN2O", 
    "kgCO2e-OtherGHGs-AR5", 
    "kgCO2e-OtherGHGs-AR4", 
    "uncertainty", 
    "scope", 
    "lca_activity", 
    "source",
    "dataset",
    "year_released",
    "year_valid_from",
    "region",
    "data_quality",
    "data_accessed",
    "description",
    "source_link"
]


def get_fuel_solid(fuel: pd.DataFrame, unit: Literal["MMBTU", "short ton"]) -> pd.DataFrame:
    fuel_solid = fuel[:22]

    cols = [str(c).lower().replace(" ", "_") for c in fuel_solid.iloc[1]] # rename cols
    cols[0] = "name"
    fuel_solid.columns = cols

    fuel_solid = fuel_solid.drop(fuel.index[[0, 1, 2, 12, 17]]) # drop nan rows

    fuel_solid[["kg_ch4_per_mmbtu", "kg_n2o_per_mmbtu", "kg_ch4_per_short_ton", "kg_n2o_per_short_ton"]] = fuel_solid[["g_ch4_per_mmbtu", "g_n2o_per_mmbtu", "g_ch4_per_short_ton", "g_n2o_per_short_ton"]] / 1000

    fuel_solid["sector"] = "Energy"
    fuel_solid["category"] = "Fuel"
    fuel_solid["activity_unit"] = unit
    fuel_solid["source"] = "EPA"
    fuel_solid["dataset"] = "GHG Emission Factors Hub"
    fuel_solid["year_released"] = 2023
    fuel_solid["year_valid_from"] = 2023
    fuel_solid["region"] = "US"
    fuel_solid["scope"] = 1
    fuel_solid["lca_activity"] = "fuel_combustion"
    fuel_solid["description"] = "Emission intensity of stationary combustion of fuel. Retrieved from the GHG Emission Factors Hub (xlsx) file published by the US EPA at the source URL."
    fuel_solid["source_link"] = "https://www.epa.gov/climateleadership/ghg-emission-factors-hub"
    fuel_solid[["kgCO2e-AR5", "kgCO2e-AR4", "kgCO2e-OtherGHGs-AR5", "kgCO2e-OtherGHGs-AR4"]] = np.nan
    fuel_solid[["uncertainty", "data_quality"]] = np.nan
    fuel_solid["data_accessed"] = datetime.now().strftime("%d/%m/%Y")
    fuel_solid["activity_id"] = fuel_solid.apply(lambda x: "fuel-type_" + x["name"].lower().replace(" ", "_") + "-fuel_use_stationary_combustion", axis=1)

    if unit == "MMBTU":
        fuel_solid = fuel_solid.rename(columns={
            "kg_co2_per_mmbtu": "kgCO2",
            "kg_ch4_per_mmbtu": "kgCH4",
            "kg_n2o_per_mmbtu": "kgN2O",
        })
    elif unit == "short ton":
        fuel_solid = fuel_solid.rename(columns={
            "kg_co2_per_short_ton": "kgCO2",
            "kg_ch4_per_short_ton": "kgCH4",
            "kg_n2o_per_short_ton": "kgN2O",
        })

    return fuel_solid[TARGET_COLS]


def get_fuel_gaseous(fuel: pd.DataFrame, unit: Literal["MMBTU", "scf"]) -> pd.DataFrame:
    fuel_gaseous = fuel[22:33].reset_index(drop=True)

    cols = [str(c).lower().replace(" ", "_") for c in fuel_gaseous.iloc[0]] # rename cols
    cols[0] = "name"
    fuel_gaseous.columns = cols

    fuel_gaseous = fuel_gaseous.drop(fuel.index[[0, 1, 3, 8]]) # drop nan rows

    fuel_gaseous[["kg_ch4_per_mmbtu", "kg_n2o_per_mmbtu", "kg_ch4_per_scf", "kg_n2o_per_scf"]] = fuel_gaseous[["g_ch4_per_mmbtu", "g_n2o_per_mmbtu", "g_ch4_per_scf", "g_n2o_per_scf"]] / 1000

    fuel_gaseous["sector"] = "Energy"
    fuel_gaseous["category"] = "Fuel"
    fuel_gaseous["activity_unit"] = unit
    fuel_gaseous["source"] = "EPA"
    fuel_gaseous["dataset"] = "GHG Emission Factors Hub"
    fuel_gaseous["year_released"] = 2023
    fuel_gaseous["year_valid_from"] = 2023
    fuel_gaseous["region"] = "US"
    fuel_gaseous["scope"] = 1
    fuel_gaseous["lca_activity"] = "fuel_combustion"
    fuel_gaseous["description"] = "Emission intensity of stationary combustion of fuel. Retrieved from the GHG Emission Factors Hub (xlsx) file published by the US EPA at the source URL."
    fuel_gaseous["source_link"] = "https://www.epa.gov/climateleadership/ghg-emission-factors-hub"
    fuel_gaseous[["kgCO2e-AR5", "kgCO2e-AR4", "kgCO2e-OtherGHGs-AR5", "kgCO2e-OtherGHGs-AR4"]] = np.nan
    fuel_gaseous[["uncertainty", "data_quality"]] = np.nan
    fuel_gaseous["data_accessed"] = datetime.now().strftime("%d/%m/%Y")
    fuel_gaseous["activity_id"] = fuel_gaseous.apply(lambda x: "fuel-type_" + x["name"].lower().replace(" ", "_") + "-fuel_use_stationary_combustion", axis=1)

    if unit == "MMBTU":
        fuel_gaseous = fuel_gaseous.rename(columns={
            "kg_co2_per_mmbtu": "kgCO2",
            "kg_ch4_per_mmbtu": "kgCH4",
            "kg_n2o_per_mmbtu": "kgN2O",
        })
    elif unit == "scf":
        fuel_gaseous = fuel_gaseous.rename(columns={
            "kg_co2_per_mmbtu": "kgCO2",
            "kg_ch4_per_scf": "kgCH4",
            "kg_n2o_per_scf": "kgN2O",
        })

    return fuel_gaseous[TARGET_COLS]



def get_fuel_petroleum(fuel: pd.DataFrame, unit: Literal["MMBTU", "gallon"]) -> pd.DataFrame:
    fuel_gaseous = fuel[33:81].reset_index(drop=True)

    cols = [str(c).lower().replace(" ", "_") for c in fuel_gaseous.iloc[0]] # rename cols
    cols[0] = "name"
    fuel_gaseous.columns = cols

    fuel_gaseous = fuel_gaseous.drop(fuel.index[[0, 1, 32, 37]]) # drop nan rows
    fuel_gaseous[["kg_ch4_per_mmbtu", "kg_n2o_per_mmbtu", "kg_ch4_per_gallon", "kg_n2o_per_gallon"]] = fuel_gaseous[["g_ch4_per_mmbtu", "g_n2o_per_mmbtu", "g_ch4_per_gallon", "g_n2o_per_gallon"]] / 1000

    fuel_gaseous["sector"] = "Energy"
    fuel_gaseous["category"] = "Fuel"
    fuel_gaseous["activity_unit"] = unit
    fuel_gaseous["source"] = "EPA"
    fuel_gaseous["dataset"] = "GHG Emission Factors Hub"
    fuel_gaseous["year_released"] = 2023
    fuel_gaseous["year_valid_from"] = 2023
    fuel_gaseous["region"] = "US"
    fuel_gaseous["scope"] = 1
    fuel_gaseous["lca_activity"] = "fuel_combustion"
    fuel_gaseous["description"] = "Emission intensity of stationary combustion of fuel. Retrieved from the GHG Emission Factors Hub (xlsx) file published by the US EPA at the source URL."
    fuel_gaseous["source_link"] = "https://www.epa.gov/climateleadership/ghg-emission-factors-hub"
    fuel_gaseous[["kgCO2e-AR5", "kgCO2e-AR4", "kgCO2e-OtherGHGs-AR5", "kgCO2e-OtherGHGs-AR4"]] = np.nan
    fuel_gaseous[["uncertainty", "data_quality"]] = np.nan
    fuel_gaseous["data_accessed"] = datetime.now().strftime("%d/%m/%Y")
    fuel_gaseous["activity_id"] = fuel_gaseous.apply(lambda x: "fuel-type_" + x["name"].lower().replace(" ", "_") + "-fuel_use_stationary_combustion", axis=1)

    if unit == "MMBTU":
        fuel_gaseous = fuel_gaseous.rename(columns={
            "kg_co2_per_mmbtu": "kgCO2",
            "kg_ch4_per_mmbtu": "kgCH4",
            "kg_n2o_per_mmbtu": "kgN2O",
        })
    elif unit == "gallon":
        fuel_gaseous = fuel_gaseous.rename(columns={
            "kg_co2_per_mmbtu": "kgCO2",
            "kg_ch4_per_gallon": "kgCH4",
            "kg_n2o_per_gallon": "kgN2O",
        })

    return fuel_gaseous[TARGET_COLS]


def get_electricity(epa: pd.DataFrame) -> pd.DataFrame:
    electricity = epa.iloc[321:350, 2:7].reset_index(drop=True) # lb / MWh
    cols = [str(c).lower().replace(" ", "_") for c in electricity.iloc[0]] # rename cols
    cols[0] = "region"
    electricity.columns = cols
    electricity["region"] = "US-" + electricity["region"]

    electricity = electricity.drop(electricity.index[[0, 1]]) # drop nan rows
    electricity[["co2_factor_kg_per_kwh", "ch4_factor_kg_per_kwh", "n2o_factor_kg_per_kwh"]] = electricity[["co2_factor", "ch4_factor", "n2o_factor"]] / 2205 # to kg / kwh

    electricity["name"] = "Electricity supplied from grid"
    electricity["sector"] = "Energy"
    electricity["category"] = "Electricity"
    electricity["activity_unit"] = "kWh"
    electricity["source"] = "EPA"
    electricity["dataset"] = "GHG Emission Factors Hub"
    electricity["year_released"] = 2023
    electricity["year_valid_from"] = 2023
    electricity["scope"] = 2
    electricity["lca_activity"] = "electricity_generation"
    electricity["description"] = "Emission intensity of the total output of electricity in the US eGrid subregion specified as reported for 2023 based on 2020 statistics. Total output emission factors can be used as default factors for estimating GHG emissions from electricity use when developing a carbon footprint or emissions inventory. Retrieved from the GHG Emission Factors Hub (xlsx) file published by the US EPA at the source URL."
    electricity["source_link"] = "https://www.epa.gov/climateleadership/ghg-emission-factors-hub"
    electricity[["kgCO2e-AR5", "kgCO2e-AR4", "kgCO2e-OtherGHGs-AR5", "kgCO2e-OtherGHGs-AR4"]] = np.nan
    electricity[["uncertainty", "data_quality"]] = np.nan
    electricity["data_accessed"] = datetime.now().strftime("%d/%m/%Y")
    electricity["activity_id"] = "electricity-supply_grid-source_supplier_mix"

    electricity = electricity.rename(columns={
        "co2_factor_kg_per_kwh": "kgCO2",
        "ch4_factor_kg_per_kwh": "kgCH4",
        "n2o_factor_kg_per_kwh": "kgN2O",
    })

    return electricity[TARGET_COLS]



def get_waste(epa: pd.DataFrame) -> pd.DataFrame:
    fuel_gaseous = epa.iloc[416:477, 2:9].reset_index(drop=True)

    cols = [str(c).lower().replace(" ", "_") for c in fuel_gaseous.iloc[0]] # rename cols
    cols[0] = "category"
    fuel_gaseous.columns = cols


    fuel_gaseous = fuel_gaseous.drop(fuel_gaseous.index[0]) # drop nan rows

    kg_cols = {
        "recycleda_kg": ("Recycled", "Emission intensity of recycling the specified material. These factors do not include any avoided emissions impact from any of the disposal methods. Recycling emissions include transport to recycling facility and sorting of recycled materials at material recovery facility. Retrieved from the EPA's GHG Emission Factors Hub (xlsx)."), 
        "landfilledb_kg": ("Landfilled", "Emission intensity of disposing of the specified material to landfill. These factors do not include any avoided emissions impact from any of the disposal methods. Landfilling emissions include transport to landfill/equipment use at landfill and fugitive landfill CH4 emissions. Landfill CH4 is based on typical landfill gas collection practices and average landfill moisture conditions. Retrieved from the EPA's GHG Emission Factors Hub (xlsx)."), 
        "combustedc_kg": ("Combusted", "Emission intensity of disposing of the specified material through combustion. These factors do not include any avoided emissions impact from any of the disposal methods. Combustion emissions include transport to combustion facility and combustion-related non-biogenic CO2 and N2O. Retrieved from the EPA's GHG Emission Factors Hub (xlsx)."), 
        "compostedd_kg": ("Composted", np.nan), 
        "anaerobically_digested_(dry_digestate_with_curing)_kg": ("Anaerobically Digested (Dry Digestate with Curing)", "Emission intensity of disposing of the specified material through anaerobic digestion. These factors do not include any avoided emissions impact from any of the disposal methods. All the factors presented here include transportation emissions (which are optional in the Scope 3 Calculation Guidance) with an assumed average distance traveled to the processing facility. Retrieved from the EPA's GHG Emission Factors Hub (xlsx)."), 
        "anaerobically_digested_(wet_digestate_with_curing)_kg": ("Anaerobically Digested (Wet Digestate with Curing)", np.nan)
    }
    fuel_gaseous[list(kg_cols.keys())] = fuel_gaseous[["recycleda", "landfilledb", "combustedc", "compostedd", "anaerobically_digested_(dry_digestate_with_curing)", "anaerobically_digested_(wet_digestate_with_curing)"]].astype(float) * 1000
    
    dfs = []
    for k, v in kg_cols.items():
        df = fuel_gaseous[["category", k]]

        df["sector"] = "Waste"
        df["name"] = df["category"] + f" - {v[0]}"
        df["activity_id"] = df.apply(lambda x: "waste-type_" + x["category"].lower().replace(" ", "_") + f"-disposal_method_{v[0].lower()}", axis=1)
        df["category"] = df["category"] + " Waste"
        df["activity_unit"] = "short ton"
        df["source"] = "EPA"
        df["dataset"] = "GHG Emission Factors Hub"
        df["year_released"] = 2023
        df["year_valid_from"] = 2023
        df["region"] = "US"
        df["scope"] = 3
        df["lca_activity"] = "end_of_life"
        df["description"] = v[1]
        df["source_link"] = "https://www.epa.gov/climateleadership/ghg-emission-factors-hub"
        df[["kgCO2e-AR5", "kgCO2", "kgCH4", "kgN2O", "kgCO2e-OtherGHGs-AR5", "kgCO2e-OtherGHGs-AR4"]] = np.nan
        df[["uncertainty", "data_quality"]] = np.nan
        df["data_accessed"] = datetime.now().strftime("%d/%m/%Y")
        df = df.rename(columns={k: "kgCO2e-AR4"})

        dfs.append(df[TARGET_COLS])

    return pd.concat(dfs)


def export_to_csv(df: pd.DataFrame, filename: str) -> None:
    df[["uncertainty", "data_quality"]] = df[["uncertainty", "data_quality"]].fillna(" ")
    df = df.fillna("not-supplied")
    df.to_csv(filename, index=False)
    


if __name__ == "__main__":
    epa = pd.read_excel(EPA_FILE, engine="openpyxl")

    fuel = epa.iloc[12:88, 2:10].reset_index(drop=True)

    fuel_solid_mmbtu = get_fuel_solid(fuel, unit="MMBTU")
    fuel_solid_short_ton = get_fuel_solid(fuel, unit="short ton")

    fuel_gaseous_mmbtu = get_fuel_gaseous(fuel, unit="MMBTU")
    fuel_gaseous_scf = get_fuel_gaseous(fuel, unit="scf")
    
    fuel_petroleum_mmbtu = get_fuel_petroleum(fuel, "MMBTU")
    fuel_petroleum_gallon = get_fuel_petroleum(fuel, "gallon")
    
    electricity = get_electricity(epa)

    waste = get_waste(epa)

    result = pd.concat([fuel_solid_mmbtu, fuel_solid_short_ton, fuel_gaseous_mmbtu, fuel_gaseous_scf, fuel_petroleum_mmbtu, fuel_petroleum_gallon, electricity, waste]).reset_index(drop=True)
    result["UUID"] = [uuid.uuid4() for _ in range(len(result.index))]
    result = result[["UUID"] + TARGET_COLS]

    export_to_csv(result, "OpenEmissionFactorsDB_task_updated.csv")
