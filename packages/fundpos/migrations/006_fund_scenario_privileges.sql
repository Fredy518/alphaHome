GRANT SELECT ON fundpos.fund_scenario_exposure TO fundpos_reader;
GRANT SELECT, INSERT, UPDATE, DELETE
ON fundpos.fund_scenario_exposure TO fundpos_writer;
GRANT ALL PRIVILEGES
ON fundpos.fund_scenario_exposure TO fundpos_migrator;
