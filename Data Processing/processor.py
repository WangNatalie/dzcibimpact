# filename: processor.py
import pandas as pd
from sqlalchemy import create_engine, text      
import logging
import argparse
import matplotlib.pyplot as plt
from database_setup import setup_database

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CIBImpactProcessor:
    def __init__(self, db_config):
        """
        Initialize processor with database configuration
        
        Args:
            db_config (dict): Database connection parameters
        """
        self.db_config = db_config
        self.engine = self._create_db_connection()
        
    def _create_db_connection(self):
        """Create SQLAlchemy engine for database connection"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:"
                f"{self.db_config['password']}@"
                f"{self.db_config['host']}:"
                f"{self.db_config['port']}/"
                f"{self.db_config['database']}"
            )
            engine = create_engine(connection_string)
            logger.info("Database connection established")
            return engine
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def create_database_schema(self):
        """Create database tables for biocapacity analysis"""
        
        # SOLRIS lookup table (UPDATED: Added 'naturalness' column)
        lookup_table_sql = """
        CREATE TABLE IF NOT EXISTS solris_lookup (
            solris_code INTEGER PRIMARY KEY,
            solris_class TEXT NOT NULL,
            biocapacity_category TEXT NOT NULL,
            biocapacity_conversion_factor DECIMAL(4,2) NOT NULL,
            lulc_category TEXT NOT NULL,
            agc_tc_ha DECIMAL(8,4) NOT NULL,
            bgc_tc_ha DECIMAL(8,4) NOT NULL,
            soc_tc_ha DECIMAL(8,4) NOT NULL,
            deoc_tc_ha DECIMAL(8,4) NOT NULL,
            naturalness DECIMAL(4,2) NOT NULL,
            description TEXT
        );
        """
        
        # Main results table
        results_table_sql = """
        CREATE TABLE IF NOT EXISTS biocapacity_results (
            id SERIAL PRIMARY KEY,
            solris_code INTEGER,
            solris_class TEXT NOT NULL,
            biocapacity_category TEXT NOT NULL,
            area_hectares DECIMAL(12,4) NOT NULL,
            biocapacity_conversion_factor DECIMAL(4,2) NOT NULL,
            biocapacity_gha DECIMAL(12,4) NOT NULL,
            percentage_of_total DECIMAL(5,2) NOT NULL,
            FOREIGN KEY(solris_code) REFERENCES solris_lookup(solris_code)
        );
        """
        
        # Carbon sequestration results table
        carbon_table_sql = """
        CREATE TABLE IF NOT EXISTS carbon_sequestration_results (
            id SERIAL PRIMARY KEY,
            solris_code INTEGER,
            solris_class TEXT NOT NULL,
            area_hectares DECIMAL(12,4) NOT NULL,
            agc_tc_ha DECIMAL(8,4) NOT NULL,
            bgc_tc_ha DECIMAL(8,4) NOT NULL,
            soc_tc_ha DECIMAL(8,4) NOT NULL,
            deoc_tc_ha DECIMAL(8,4) NOT NULL,
            total_carbon_tc DECIMAL(12,4) NOT NULL,
            ssc DECIMAL(12,4) NOT NULL,
            ssc_density DECIMAL(12,6) NOT NULL,
            percentage_of_total DECIMAL(5,2) NOT NULL,
            FOREIGN KEY(solris_code) REFERENCES solris_lookup(solris_code)
        );
        """

        # Water filtration results table
        water_table_sql = """
        CREATE TABLE IF NOT EXISTS water_filtration_results (
            id SERIAL PRIMARY KEY,
            solris_code INTEGER,
            solris_class TEXT NOT NULL,
            area_hectares DECIMAL(12,4) NOT NULL,
            wf_value_per_ha DECIMAL(12,4) NOT NULL,
            total_wf_value DECIMAL(14,4) NOT NULL,
            percentage_of_total DECIMAL(5,2) NOT NULL,
            FOREIGN KEY(solris_code) REFERENCES solris_lookup(solris_code)
        );
        """

        # NEW: Aesthetic quality results table
        aesthetic_table_sql = """
        CREATE TABLE IF NOT EXISTS aesthetic_quality_results (
            id SERIAL PRIMARY KEY,
            solris_code INTEGER,
            solris_class TEXT NOT NULL,
            area_hectares DECIMAL(12,4) NOT NULL,
            naturalness_score DECIMAL(4,2) NOT NULL,
            rarity_score INTEGER NOT NULL,
            aesthetic_quality_score DECIMAL(5,2) NOT NULL,
            FOREIGN KEY(solris_code) REFERENCES solris_lookup(solris_code)
        );
        """
        
        with self.engine.connect() as conn:
            # Create tables if they do not exist
            conn.execute(text(lookup_table_sql))
            conn.execute(text(results_table_sql))
            conn.execute(text(carbon_table_sql))
            conn.execute(text(water_table_sql))
            conn.execute(text(aesthetic_table_sql)) # NEW
            conn.commit()
            logger.info("Database schema created successfully")

    def clear_results_for_mode(self, mode):
        """Clear only the results table for the specified processing mode"""
        # UPDATED: Added 'aesthetic_quality'
        table_map = {
            'biocapacity': 'biocapacity_results',
            'carbon': 'carbon_sequestration_results',
            'carbon_sequestration': 'carbon_sequestration_results',
            'water': 'water_filtration_results',
            'water_filtration': 'water_filtration_results',
            'aesthetic_quality': 'aesthetic_quality_results'
        }
        table_name = table_map.get(mode)
        if not table_name:
            raise ValueError(f"Unknown mode for clearing results: {mode}")
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP TABLE {table_name};"))
            conn.commit()
            logger.info(f"Cleared results table: {table_name}")
    
    def clear_all_data(self):
        """Clear all data from tables in the correct order to respect foreign key constraints"""
        with self.engine.connect() as conn:
            # Clear dependent tables first (in reverse dependency order)
            # UPDATED: Added aesthetic_quality_results
            conn.execute(text("DROP TABLE aesthetic_quality_results;"))
            conn.execute(text("DROP TABLE carbon_sequestration_results;"))
            conn.execute(text("DROP TABLE water_filtration_results;"))
            conn.execute(text("DROP TABLE biocapacity_results;"))
            conn.execute(text("DROP TABLE solris_lookup;"))
            conn.commit()
            logger.info("All database tables cleared")
    
    def load_solris_lookup_table(self, csv_path, custom_factors=None):
        """
        Load SOLRIS classification lookup table from CSV file into database
        
        Args:
            csv_path (str): Path to the CSV file containing SOLRIS lookup data
            custom_factors (dict): Custom conversion ratios to override defaults
        """
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded CSV file with {len(df)} records")
            
            # Validate required columns (UPDATED: Added 'naturalness')
            required_columns = ['solris_code', 'solris_class', 'biocapacity_category', 
                              'biocapacity_conversion_factor', 'lulc_category', 'agc_tc_ha', 
                              'bgc_tc_ha', 'soc_tc_ha', 'deoc_tc_ha', 'naturalness', 'description']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in CSV: {missing_columns}")
            
            # Apply custom factors if provided
            if custom_factors:
                for code, factors in custom_factors.items():
                    mask = df['solris_code'] == code
                    if 'biocapacity_conversion_factor' in factors:
                        df.loc[mask, 'biocapacity_conversion_factor'] = factors['biocapacity_conversion_factor']
                        logger.info(f"Applied custom conversion ratio for SOLRIS code {code}")
            
            # Drop all rows with any null values before inserting
            df = df.dropna()
            
            # Clear existing data and insert new
            with self.engine.connect() as conn:
                try:
                    conn.execute(text("DELETE FROM solris_lookup;"))
                    conn.commit()
                    logger.info("Cleared existing data from solris_lookup table")
                except Exception as e:
                    # If deletion fails due to foreign key constraints, clear dependent tables first
                    error_msg = str(e)
                    if "violates foreign key constraint" in error_msg.lower() or "still referenced" in error_msg.lower() or "depends on" in error_msg.lower():
                        logger.info("Clearing dependent tables first due to foreign key constraints...")
                        # Clear all results tables first (they depend on solris_lookup)
                        # Handle each table independently in case some don't exist
                        tables_to_clear = [
                            'biocapacity_results',
                            'carbon_sequestration_results',
                            'water_filtration_results',
                            'aesthetic_quality_results'
                        ]
                        for table in tables_to_clear:
                            try:
                                conn.execute(text(f"TRUNCATE TABLE {table};"))
                                logger.debug(f"Cleared {table}")
                            except Exception as truncate_error:
                                logger.debug(f"Table {table} may not exist yet: {truncate_error}")
                        conn.commit()
                        logger.info("Cleared all existing results tables")
                        
                        conn.execute(text("DELETE FROM solris_lookup;"))
                        conn.commit()
                        logger.info("Cleared existing data from solris_lookup table")
                    else:
                        raise
            
            # Load data to database
            df.to_sql('solris_lookup', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(df)} SOLRIS classifications into lookup table")
            
            return df
            
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            raise

    # ... (existing process_biocapacity_data, process_carbon_sequestration, process_water_filtration methods remain unchanged) ...

    def process_biocapacity_data(self, excel_path):
        """Process Excel data and calculate biocapacity"""
        df = pd.read_excel(excel_path)

        # Ensure required columns exist
        required_cols = ["gridcode", "SUM_Area_Ha"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Aggregate by land cover code
        agg_df = df.groupby("gridcode")["SUM_Area_Ha"].sum().reset_index()
        agg_df.rename(columns={"SUM_Area_Ha": "area_hectares"}, inplace=True)

        # Get biocapacity conversion ratios from lookup table
        lookup_df = pd.read_sql("SELECT * FROM solris_lookup", self.engine)

        # Merge Excel data with lookup table
        merged = agg_df.merge(lookup_df, left_on="gridcode", right_on="solris_code", how="left")

        # Check for missing lookup entries
        missing_codes = merged[merged['solris_class'].isna()]['gridcode'].unique()
        if len(missing_codes) > 0:
            logger.warning(f"Missing lookup entries for SOLRIS codes: {missing_codes}")

        # Calculate biocapacity using conversion ratio
        # Biocapacity (gha) = Area (ha) × Conversion Ratio (gha/ha)
        merged["biocapacity_gha"] = merged["area_hectares"] * merged["biocapacity_conversion_factor"]
        
        # Calculate percentage of total biocapacity
        total_biocapacity = merged["biocapacity_gha"].sum()
        merged["percentage_of_total"] = (merged["biocapacity_gha"] / total_biocapacity * 100)

        return merged
    
    def process_carbon_sequestration(self, excel_path):
        """Process Excel data and calculate carbon sequestration"""
        df = pd.read_excel(excel_path)

        # Ensure required columns exist
        required_cols = ["gridcode", "SUM_Area_Ha"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Aggregate by land cover code
        agg_df = df.groupby("gridcode")["SUM_Area_Ha"].sum().reset_index()
        agg_df.rename(columns={"SUM_Area_Ha": "area_hectares"}, inplace=True)

        # Get SOLRIS classifications and carbon values from lookup table
        lookup_df = pd.read_sql(
            "SELECT solris_code, solris_class, agc_tc_ha, bgc_tc_ha, soc_tc_ha, deoc_tc_ha FROM solris_lookup",
            self.engine
        )

        # Merge Excel data with lookup table
        merged = agg_df.merge(lookup_df, left_on="gridcode", right_on="solris_code", how="left")

        # Check for missing lookup entries
        missing_codes = merged[merged['solris_class'].isna()]['gridcode'].unique()
        if len(missing_codes) > 0:
            logger.warning(f"Missing lookup entries for SOLRIS codes: {missing_codes}")

        # Calculate total carbon sequestration
        # Total Carbon (tc) = (agc + bgc + soc + deoc) × Area (ha)
        merged["total_carbon_tc"] = (
            merged["agc_tc_ha"].fillna(0) + merged["bgc_tc_ha"].fillna(0) + merged["soc_tc_ha"].fillna(0) + merged["deoc_tc_ha"].fillna(0)
        ) * merged["area_hectares"]

        # Add social cost of carbon (ssc) column (PV 2021)
        merged["ssc"] = merged["total_carbon_tc"] * 252

        # Calculate percentage of total carbon sequestration
        total_carbon = merged["total_carbon_tc"].sum()
        merged["percentage_of_total"] = (merged["total_carbon_tc"] / total_carbon * 100) if total_carbon != 0 else 0

        return merged

    def process_water_filtration(self, excel_path, water_csv_path='water-filtration.csv'):
        """Process Excel data and map wetland water filtration values to SOLRIS classes"""
        df = pd.read_excel(excel_path)

        # Ensure required columns exist
        required_cols = ["gridcode", "SUM_Area_Ha"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Aggregate by land cover code
        agg_df = df.groupby("gridcode")["SUM_Area_Ha"].sum().reset_index()
        agg_df.rename(columns={"SUM_Area_Ha": "area_hectares"}, inplace=True)

        # Get SOLRIS classifications
        lookup_df = pd.read_sql(
            "SELECT solris_code, solris_class FROM solris_lookup",
            self.engine
        )

        # Merge Excel data with lookup to get SOLRIS class names
        merged = agg_df.merge(lookup_df, left_on="gridcode", right_on="solris_code", how="left")

        # Load water filtration CSV (wetland_type,value)
        wf_df = pd.read_csv(water_csv_path)

        # Normalize column names for join
        wf_df = wf_df.rename(columns={"wetland_type": "solris_class", "value": "wf_value_per_ha"})

        # Join by solris_class to attach water filtration value per ha
        merged = merged.merge(wf_df, on="solris_class", how="left")

        # Compute total water filtration value (0 if no value defined for class)
        merged["wf_value_per_ha"] = merged["wf_value_per_ha"].fillna(0)
        merged["total_wf_value"] = (merged["area_hectares"] * merged["wf_value_per_ha"]).round(4)

        # Calculate percentage of total water filtration value
        total_wf = merged["total_wf_value"].sum()
        merged["percentage_of_total"] = (
            (merged["total_wf_value"] / total_wf * 100).fillna(0)
            if total_wf != 0 else 0
        )

        return merged
        
    def process_aesthetic_quality(self, excel_path):
        """NEW: Process land cover data to calculate aesthetic quality score."""
        df = pd.read_excel(excel_path)

        # Ensure required columns exist
        required_cols = ["gridcode", "SUM_Area_Ha"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Aggregate by land cover code
        agg_df = df.groupby("gridcode")["SUM_Area_Ha"].sum().reset_index()
        agg_df.rename(columns={"SUM_Area_Ha": "area_hectares"}, inplace=True)

        # Get SOLRIS class and naturalness score from lookup table
        lookup_df = pd.read_sql(
            "SELECT solris_code, solris_class, naturalness FROM solris_lookup",
            self.engine
        )
        lookup_df = lookup_df.rename(columns={"naturalness": "naturalness_score"})

        # Merge Excel data with lookup
        merged = agg_df.merge(lookup_df, left_on="gridcode", right_on="solris_code", how="left")
        merged.dropna(subset=['solris_class'], inplace=True) # Drop codes not in lookup

        # --- Rarity Calculation (Revised Method) ---
        total_study_area = merged['area_hectares'].sum()
        merged['percentage_of_total'] = (merged['area_hectares'] / total_study_area) * 100
        bins = [-float('inf'), 1,        5,         15,        30,         float('inf')]
        labels = [5, 4, 3, 2, 1]

        merged['rarity_score'] = pd.cut(
            merged['percentage_of_total'],
            bins=bins,
            labels=labels,
            right=True # Means the right side of the bin is inclusive (e.g., 5% falls in the (1, 5] bin)
        ).astype(int)

        naturalness_weight = 0.67
        rarity_weight = 0.33

        merged['aesthetic_quality_score'] = (
            (merged['naturalness_score'] * naturalness_weight) +
            (merged['rarity_score'] * rarity_weight)
        )

        return merged

    def save_results_to_database(self, results_df, type):
        """Save processing results to database"""
        
        if type == 'biocapacity':
            # Select only needed columns for database
            results_cols = [
                'solris_code', 'solris_class',
                'biocapacity_category', 'area_hectares', 'biocapacity_conversion_factor', 
                'biocapacity_gha', 'percentage_of_total'
            ]
            # Round area_hectares to 4 decimal places
            results_df['area_hectares'] = results_df['area_hectares'].round(4)
            # Save to database
            results_df[results_cols].to_sql(
                'biocapacity_results', self.engine, 
                if_exists='append', index=False
            )
        
        elif type == 'carbon':
            # Select only needed columns for database
            results_cols = [
                'solris_code', 'solris_class',
                'area_hectares', 'agc_tc_ha', 'bgc_tc_ha', 'soc_tc_ha', 'deoc_tc_ha',
                'total_carbon_tc', 'ssc', 'percentage_of_total'
            ]
            # Round area_hectares and ssc to 4 decimal places, and store ssc in millions
            results_df['area_hectares'] = results_df['area_hectares'].round(4)
            results_df['ssc'] = (results_df['ssc'] / 1_000_000).round(4)
            # Add SSC density (million $ per hectare), round to 6 decimals
            results_df['ssc_density'] = (results_df['ssc'] / results_df['area_hectares']).replace([float('inf'), -float('inf')], 0).fillna(0).round(6)
            # Save to database
            results_df[results_cols + ['ssc_density']].to_sql(
                'carbon_sequestration_results', self.engine, 
                if_exists='append', index=False
            )
        elif type == 'water':
            # Select columns for water filtration
            results_cols = [
                'solris_code', 'solris_class',
                'area_hectares', 'wf_value_per_ha', 'total_wf_value', 'percentage_of_total'
            ]
            # Round area_hectares to 4 decimal places
            results_df['area_hectares'] = results_df['area_hectares'].round(4)
            # Save to database
            results_df[results_cols].to_sql(
                'water_filtration_results', self.engine,
                if_exists='append', index=False
            )
        
        # NEW: Save aesthetic quality results
        elif type == 'aesthetic':
            results_cols = [
                'solris_code', 'solris_class', 'area_hectares', 'naturalness_score',
                'rarity_score', 'aesthetic_quality_score'
            ]
            results_df['area_hectares'] = results_df['area_hectares'].round(4)
            results_df[results_cols].to_sql(
                'aesthetic_quality_results', self.engine,
                if_exists='append', index=False
            )
            
        logger.info(f"{type} results saved to database successfully")
    
    # ... (existing generate_biocapacity_report, generate_water_filtration_report, generate_carbon_report methods remain unchanged) ...
    def generate_biocapacity_report(self, study_area_name):
        """Generate summary report for a study area"""
        
        # Get results data grouped by SOLRIS class
        results_query = """
        SELECT 
            solris_class,
            SUM(area_hectares) as total_area_hectares,
            SUM(biocapacity_gha) as total_biocapacity_gha
        FROM biocapacity_results 
        GROUP BY solris_class
        ORDER BY total_biocapacity_gha DESC
        """
        
        results_df = pd.read_sql(results_query, self.engine)
        
        # Calculate percentages
        total_biocapacity = results_df['total_biocapacity_gha'].sum()
        total_area = results_df['total_area_hectares'].sum()
        results_df['percentage_of_total'] = (
            results_df['total_biocapacity_gha'] / total_biocapacity * 100
        )
        
        # Generate report
        report = f"""
        BIOCAPACITY ANALYSIS REPORT
        Study Area: {study_area_name}
        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

        {'='*50}
        SUMMARY BY SOLRIS CLASS
        {'='*50}

        """
        
        for _, row in results_df.iterrows():
            report += f"""
            {row['solris_class']}:
            Area: {row['total_area_hectares']:,.2f} hectares ({row['total_area_hectares']/total_area*100:.1f}% of total)
            Biocapacity: {row['total_biocapacity_gha']:,.2f} global hectares ({row['percentage_of_total']:.1f}% of total)
            """
        
        report += f"""
        {'='*50}
        TOTALS
        {'='*50}
        Total Area: {total_area:,.2f} hectares
        Total Biocapacity: {total_biocapacity:,.2f} global hectares
        Biocapacity per Hectare: {total_biocapacity/total_area:.3f} gha/ha
        """
        
        return report

    def generate_water_filtration_report(self, study_area_name):
        """Generate water filtration summary report for a study area"""
        results_query = """
        SELECT 
            solris_class,
            SUM(area_hectares) as total_area_hectares,
            AVG(wf_value_per_ha) as wf_value_per_ha,
            SUM(total_wf_value) as total_wf_value
        FROM water_filtration_results
        GROUP BY solris_class
        ORDER BY total_wf_value DESC
        """

        results_df = pd.read_sql(results_query, self.engine)

        total_area = results_df['total_area_hectares'].sum() if not results_df.empty else 0
        total_wf = results_df['total_wf_value'].sum() if not results_df.empty else 0

        report = f"""
        WATER FILTRATION ANALYSIS REPORT
        Study Area: {study_area_name}
        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

        {'='*60}
        SUMMARY BY SOLRIS CLASS
        {'='*60}

        """

        for _, row in results_df.iterrows():
            pct_area = (row['total_area_hectares']/total_area*100) if total_area else 0
            pct_wf = (row['total_wf_value']/total_wf*100) if total_wf else 0
            report += f"""
            {row['solris_class']}:
            Area: {row['total_area_hectares']:,.2f} hectares ({pct_area:.1f}% of total)
            WF Value($)/ha: {row['wf_value_per_ha']:,.2f}
            Total WF Value($): {row['total_wf_value']:,.2f} ({pct_wf:.1f}% of total)
            """

        report += f"""
        {'='*60}
        TOTALS
        {'='*60}
        Total Area: {total_area:,.2f} hectares
        Total Water Filtration Value ($ millions CAD): {total_wf/1e6:,.6f}
        """

        return report

    def generate_carbon_report(self, study_area_name):
        """Generate carbon sequestration summary report for a study area"""
        
        # Get results data grouped by SOLRIS class
        results_query = """
        SELECT 
            solris_class,
            SUM(area_hectares) as total_area_hectares,
            SUM(total_carbon_tc) as total_carbon_tc,
            AVG(agc_tc_ha) as avg_agc_tc_ha,
            AVG(bgc_tc_ha) as avg_bgc_tc_ha,
            AVG(soc_tc_ha) as avg_soc_tc_ha,
            AVG(deoc_tc_ha) as avg_deoc_tc_ha,
            SUM(ssc) as total_ssc,
            SUM(ssc_density) as total_ssc_density
        FROM carbon_sequestration_results 
        GROUP BY solris_class
        ORDER BY total_carbon_tc DESC
        """
        
        results_df = pd.read_sql(results_query, self.engine)
        
        # Calculate percentages
        total_carbon = results_df['total_carbon_tc'].sum()
        total_area = results_df['total_area_hectares'].sum()
        total_ssc = results_df['total_ssc'].sum()
        results_df['percentage_of_total'] = (
            results_df['total_carbon_tc'] / total_carbon * 100
        )
        
        # Generate report
        report = f"""
        CARBON SEQUESTRATION ANALYSIS REPORT
        Study Area: {study_area_name}
        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

        {'='*60}
        SUMMARY BY SOLRIS CLASS
        {'='*60}

        """
        
        for _, row in results_df.iterrows():
            report += f"""
            {row['solris_class']}:
            Area: {row['total_area_hectares']:,.2f} hectares ({row['total_area_hectares']/total_area*100:.1f}% of total)
            Total Carbon: {row['total_carbon_tc']:,.2f} tonnes C ({row['percentage_of_total']:.1f}% of total)
            Carbon Density: {row['total_carbon_tc']/row['total_area_hectares']:.2f} tC/ha
            Breakdown per hectare:
              - AGC: {row['avg_agc_tc_ha']:.2f} tC/ha
              - BGC: {row['avg_bgc_tc_ha']:.2f} tC/ha  
              - SOC: {row['avg_soc_tc_ha']:.2f} tC/ha
              - DeOC: {row['avg_deoc_tc_ha']:.2f} tC/ha
              - SSC: ${1000000 * row['total_ssc_density']:.2f} $CAD/ha
            Total SSC: ${row['total_ssc']:,.6f} million CAD
            """
        
        report += f"""
        {'='*60}
        TOTALS
        {'='*60}
        Total Area: {total_area:,.2f} hectares
        Total Carbon Sequestration: {total_carbon:,.2f} tonnes C
        Average Carbon Density: {total_carbon/total_area:.2f} tC/ha
        Total SSC: ${total_ssc:,.2f} million CAD
        """
        
        return report

    def generate_aesthetic_quality_report(self, study_area_name):
        """NEW: Generate aesthetic quality summary report."""
        query = """
        SELECT
            solris_class, area_hectares, naturalness_score, rarity_score, aesthetic_quality_score
        FROM aesthetic_quality_results
        ORDER BY aesthetic_quality_score DESC
        """
        results_df = pd.read_sql(query, self.engine)
        total_area = results_df['area_hectares'].sum()

        report = f"""
        AESTHETIC QUALITY ANALYSIS REPORT
        Study Area: {study_area_name}
        Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

        {'='*60}
        SUMMARY BY SOLRIS CLASS
        {'='*60}
        """

        for _, row in results_df.iterrows():
            area_pct = (row['area_hectares'] / total_area * 100) if total_area > 0 else 0
            report += f"""
            {row['solris_class']}:
            Aesthetic Score: {row['aesthetic_quality_score']:.2f}
              - Area: {row['area_hectares']:,.2f} hectares ({area_pct:.1f}% of total)
              - Naturalness Score: {row['naturalness_score']:.2f}
              - Rarity Score: {row['rarity_score']} (5=rarest, 1=most common)
            """

        # Calculate overall weighted average score
        weighted_avg_score = (results_df['aesthetic_quality_score'] * results_df['area_hectares']).sum() / total_area if total_area > 0 else 0

        report += f"""
        {'='*60}
        TOTALS
        {'='*60}
        Total Area: {total_area:,.2f} hectares
        Area-Weighted Average Aesthetic Score: {weighted_avg_score:.2f}
        """
        return report

    # ... (existing plot_discounted_social_cost method remains unchanged) ...
    def plot_discounted_social_cost(self, study_area_name, start_year=2020, end_year=2080, discount_rate=0.02, save_path=None):
        """
        Plot the present value (2021) of the total social cost of carbon (in millions $) from start_year to end_year at a given discount rate.
        Uses the total SSC (in millions) from the carbon_sequestration_results table and maps to annual SCC values.
        """
        # Get total SSC (in millions) from database
        query = """
        SELECT SUM(ssc) as total_ssc_millions FROM carbon_sequestration_results
        """
        result = pd.read_sql(query, self.engine)
        total_ssc_millions = result['total_ssc_millions'].iloc[0] if not result.empty else 0

        # Load annual SCC values from CSV
        scc_df = pd.read_csv('carbon_sequestration/annual-scc.csv')
        # Clean SCC values (remove $ and convert to float)
        scc_df['SCC'] = scc_df['SCC'].str.replace('$', '').str.replace(',', '').astype(float)
        
        # Create a mapping of year to SCC value
        scc_by_year = dict(zip(scc_df['Year'], scc_df['SCC']))
        
        # Calculate the scaling factor (total_ssc_millions divided by 2021 SCC value of 252)
        scaling_factor = total_ssc_millions / 252
        
        years = list(range(start_year, end_year + 1))
        # Map to annual SCC values and apply discounting
        discounted_ssc = []
        for year in years:
            if year in scc_by_year:
                annual_scc_value = scc_by_year[year]
                # Scale by the factor and apply discounting
                discounted_value = (scaling_factor * annual_scc_value) / ((1 + discount_rate) ** (year - start_year))
                discounted_ssc.append(discounted_value)
            else:
                # If year not in SCC data, use the last available value
                last_year = max([y for y in scc_by_year.keys() if y <= year])
                annual_scc_value = scc_by_year[last_year]
                discounted_value = (scaling_factor * annual_scc_value) / ((1 + discount_rate) ** (year - start_year))
                discounted_ssc.append(discounted_value)

        plt.figure(figsize=(10, 6))
        plt.plot(years, discounted_ssc, marker='o')
        plt.title(f"Present Value (2021) of Discounted Social Cost of Carbon ({study_area_name})\n{start_year}-{end_year} at {discount_rate*100:.1f}% Discount Rate")
        plt.xlabel("Year")
        plt.ylabel("Present Value (2021) of Social Cost (million $)")
        plt.grid(True)
        if save_path:
            plt.savefig(save_path, bbox_inches='tight')

    def export_results_to_csv(self, output_path, type):
        """Export all results to CSV file"""
        
        if type == 'biocapacity':
            # Get all results from database
            query = """
            SELECT 
                solris_class,
                solris_code,
                biocapacity_category,
                area_hectares,
                biocapacity_conversion_factor,
                biocapacity_gha,
                percentage_of_total
            FROM biocapacity_results 
            ORDER BY biocapacity_gha DESC
            """
        
        elif type == 'carbon':
            # Get all results from database
            query = """
            SELECT 
                solris_class,
                solris_code,
                area_hectares,
                agc_tc_ha,
                bgc_tc_ha,
                soc_tc_ha,
                deoc_tc_ha,
                total_carbon_tc,
                ssc,
                ssc_density,
                percentage_of_total
            FROM carbon_sequestration_results 
            ORDER BY total_carbon_tc DESC
            """
        elif type == 'water':
            query = """
            SELECT
                solris_class,
                solris_code,
                area_hectares,
                wf_value_per_ha,
                total_wf_value,
                percentage_of_total
            FROM water_filtration_results
            ORDER BY total_wf_value DESC
            """
        
        # NEW: Export aesthetic quality results
        elif type == 'aesthetic':
            query = """
            SELECT
                solris_class, solris_code, area_hectares, naturalness_score, 
                rarity_score, aesthetic_quality_score
            FROM aesthetic_quality_results
            ORDER BY aesthetic_quality_score DESC
            """
        
        results_df = pd.read_sql(query, self.engine)
        if 'solris_code' in results_df.columns:
            results_df['solris_code'] = results_df['solris_code'].astype('Int64')
        
        # Write to CSV
        results_df.to_csv(output_path, index=False)
        logger.info(f"Results ({type}) exported to CSV: {output_path}")
        
        return results_df


def main():
    """Main processing function"""
    
    # Parse command line arguments
    # UPDATED: Added 'aesthetic_quality' and 'reindex' modes
    parser = argparse.ArgumentParser(description='Process biocapacity, carbon sequestration, or water filtration data')
    parser.add_argument('--mode', choices=['biocapacity', 'carbon_sequestration', 'water_filtration', 'aesthetic_quality', 'reindex'], required=True,
                       help='Processing mode: biocapacity, carbon, water, aesthetic quality, or reindex SOLRIS table')
    parser.add_argument('--excel-path', default='carolinian_polygon_summary.xlsx',
                       help='Path to Excel file with data')
    parser.add_argument('--study-area', default='carolinian_zone',
                       help='Name of the study area')
    parser.add_argument('--csv-path', default='solris_lookup.csv',
                       help='Path to SOLRIS lookup CSV file')
    parser.add_argument('--water-csv-path', default='water_filtration_lookup.csv',
                       help='Path to water filtration CSV file')
    
    args = parser.parse_args()
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'carolinian_zone',
        'user': 'nataliewang',
        'password': 'dzcibimpact'
    }
    
    # Ensure database exists before connecting
    logger.info("Checking if database exists...")
    if not setup_database(db_config, create_user=False):
        logger.error("Failed to set up database. Please check your PostgreSQL connection and credentials.")
        return
    
    # Initialize processor
    processor = CIBImpactProcessor(db_config)
    
    # Create/update database schema (idempotent)
    processor.create_database_schema()

    if args.mode == 'reindex':
        logger.info("--- Starting SOLRIS Lookup Table Re-indexing ---")
        processor.load_solris_lookup_table(args.csv_path)
        logger.info("--- Re-indexing Complete ---")
        return # Exit after reindexing
    
    # Load lookup table from CSV file (for other modes)
    csv_path = args.csv_path
    
    # Optional: Custom factors to override specific conversion ratios
    custom_factors = {
        # Example: Override factors for specific codes
        # 33: {'biocapacity_conversion_factor': 0.60},
    }
    
    # Ensure lookup table is loaded before processing
    processor.load_solris_lookup_table(csv_path, custom_factors)
    
    if args.mode == 'biocapacity':
        # Clear only this mode's results
        processor.clear_results_for_mode('biocapacity')
        # Process biocapacity data
        results_df = processor.process_biocapacity_data(args.excel_path)
        
        # Save results to database
        processor.save_results_to_database(results_df, 'biocapacity')
        
        # Export results to CSV
        csv_output_path = f"biocapacity/biocapacity_results_{args.study_area}.csv"
        processor.export_results_to_csv(csv_output_path, 'biocapacity')
        
        # Generate and print report
        report = processor.generate_biocapacity_report(args.study_area)
        print(report)
        
        # Optionally save report to file
        with open(f"biocapacity/biocapacity_report_{args.study_area.replace(' ', '_')}.txt", 'w') as f:
            f.write(report)
            
    elif args.mode == 'carbon_sequestration':
        # Clear only this mode's results
        processor.clear_results_for_mode('carbon_sequestration')
        # Process carbon sequestration data
        results_df = processor.process_carbon_sequestration(args.excel_path)
        
        # Save results to database
        processor.save_results_to_database(results_df, 'carbon')
        
        # Export results to CSV
        csv_output_path = f"carbon_sequestration/carbon_sequestration_results_{args.study_area}.csv"
        processor.export_results_to_csv(csv_output_path, 'carbon')
        
        # Generate and print report
        report = processor.generate_carbon_report(args.study_area)
        print(report)
        
        # Optionally save report to file
        with open(f"carbon_sequestration/carbon_report_{args.study_area.replace(' ', '_')}.txt", 'w') as f:
            f.write(report)
        # Generate and save discounted social cost of carbon plot
        plot_path = f"carbon_sequestration/discounted_ssc_{args.study_area.replace(' ', '_')}.png"
        processor.plot_discounted_social_cost(args.study_area, save_path=plot_path)

    elif args.mode == 'water_filtration':
        # Clear only this mode's results
        processor.clear_results_for_mode('water_filtration')
        # Process water filtration data
        results_df = processor.process_water_filtration(args.excel_path, args.water_csv_path)

        # Save results to database
        processor.save_results_to_database(results_df, 'water')

        # Export results to CSV
        csv_output_path = f"water_filtration/water_filtration_results_{args.study_area}.csv"
        processor.export_results_to_csv(csv_output_path, 'water')

        # Generate and print report
        report = processor.generate_water_filtration_report(args.study_area)
        print(report)

        # Optionally save report to file
        with open(f"water_filtration/water_filtration_report_{args.study_area.replace(' ', '_')}.txt", 'w') as f:
            f.write(report)

    elif args.mode == 'aesthetic_quality':
        processor.clear_results_for_mode('aesthetic_quality')
        results_df = processor.process_aesthetic_quality(args.excel_path)
        processor.save_results_to_database(results_df, 'aesthetic')
        
        csv_output_path = f"aesthetic_quality/aesthetic_quality_results_{args.study_area}.csv"
        processor.export_results_to_csv(csv_output_path, 'aesthetic')
        
        report = processor.generate_aesthetic_quality_report(args.study_area)
        print(report)
        
        with open(f"aesthetic_quality/aesthetic_quality_report_{args.study_area.replace(' ', '_')}.txt", 'w') as f:
            f.write(report)


if __name__ == "__main__":
    main()