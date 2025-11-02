-- Credit Score Comparison Database Schema
-- PostgreSQL DDL for storing merged credit score data

-- Drop table if exists (for fresh start)
DROP TABLE IF EXISTS credit_score_comparison CASCADE;

-- Create main comparison table
CREATE TABLE credit_score_comparison (
    -- Identifiers
    loan_identifier VARCHAR(50) PRIMARY KEY,
    acquisition_quarter VARCHAR(10) NOT NULL,
    
    -- VantageScore 4.0 variants
    vs4_current_method INTEGER,
    vs4_trimerge INTEGER,
    vs4_bimerge_lowest INTEGER,
    vs4_bimerge_median INTEGER,
    vs4_bimerge_highest INTEGER,
    
    -- Traditional FICO score
    fico_score INTEGER,
    
    -- Loan characteristics
    original_ltv NUMERIC(5,2),
    original_dti NUMERIC(5,2),
    original_upb NUMERIC(12,2),
    original_interest_rate NUMERIC(6,3),
    loan_purpose VARCHAR(20),
    property_type VARCHAR(20),
    number_of_units INTEGER,
    occupancy_status VARCHAR(20),
    first_time_homebuyer_flag VARCHAR(1),
    
    -- Derived features
    score_difference INTEGER,           -- vs4_trimerge - fico_score
    score_ratio NUMERIC(5,3),           -- vs4_trimerge / fico_score
    score_abs_diff INTEGER,             -- ABS(score_difference)
    high_ltv_flag BOOLEAN,              -- ltv > 80
    high_dti_flag BOOLEAN,              -- dti > 43
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_fico_score ON credit_score_comparison(fico_score);
CREATE INDEX idx_vs4_trimerge ON credit_score_comparison(vs4_trimerge);
CREATE INDEX idx_acquisition_quarter ON credit_score_comparison(acquisition_quarter);
CREATE INDEX idx_score_difference ON credit_score_comparison(score_difference);
CREATE INDEX idx_loan_purpose ON credit_score_comparison(loan_purpose);
CREATE INDEX idx_ltv ON credit_score_comparison(original_ltv);
CREATE INDEX idx_dti ON credit_score_comparison(original_dti);

-- Create composite indexes for common query patterns
CREATE INDEX idx_fico_vs4 ON credit_score_comparison(fico_score, vs4_trimerge);
CREATE INDEX idx_quarter_fico ON credit_score_comparison(acquisition_quarter, fico_score);

-- Add constraints
ALTER TABLE credit_score_comparison
    ADD CONSTRAINT chk_fico_range CHECK (fico_score >= 300 AND fico_score <= 850),
    ADD CONSTRAINT chk_vs4_range CHECK (vs4_trimerge >= 300 AND vs4_trimerge <= 850),
    ADD CONSTRAINT chk_ltv_range CHECK (original_ltv >= 0 AND original_ltv <= 100),
    ADD CONSTRAINT chk_dti_range CHECK (original_dti >= 0 AND original_dti <= 100);

-- Create summary statistics view
CREATE OR REPLACE VIEW credit_score_summary AS
SELECT 
    acquisition_quarter,
    COUNT(*) as loan_count,
    AVG(fico_score) as avg_fico,
    AVG(vs4_trimerge) as avg_vs4,
    AVG(score_difference) as avg_score_diff,
    STDDEV(score_difference) as stddev_score_diff,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY score_difference) as p25_score_diff,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY score_difference) as median_score_diff,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY score_difference) as p75_score_diff,
    CORR(fico_score, vs4_trimerge) as fico_vs4_correlation
FROM credit_score_comparison
GROUP BY acquisition_quarter
ORDER BY acquisition_quarter;

-- Create view for score divergence analysis
CREATE OR REPLACE VIEW score_divergence AS
SELECT 
    loan_identifier,
    fico_score,
    vs4_trimerge,
    score_difference,
    CASE 
        WHEN ABS(score_difference) > 50 THEN 'High Divergence'
        WHEN ABS(score_difference) > 25 THEN 'Medium Divergence'
        ELSE 'Low Divergence'
    END as divergence_category,
    original_ltv,
    original_dti,
    loan_purpose
FROM credit_score_comparison
WHERE score_difference IS NOT NULL;

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON credit_score_comparison TO airflow;
-- GRANT SELECT ON credit_score_summary, score_divergence TO airflow;

-- Comments for documentation
COMMENT ON TABLE credit_score_comparison IS 'Merged dataset comparing FICO and VantageScore 4.0 credit scores for Fannie Mae loans';
COMMENT ON COLUMN credit_score_comparison.vs4_trimerge IS 'VantageScore 4.0 calculated from all three credit bureaus';
COMMENT ON COLUMN credit_score_comparison.fico_score IS 'Traditional FICO credit score';
COMMENT ON COLUMN credit_score_comparison.score_difference IS 'Difference between VS4 trimerge and FICO (positive means VS4 higher)';
COMMENT ON VIEW credit_score_summary IS 'Quarterly summary statistics for credit score comparison';
COMMENT ON VIEW score_divergence IS 'Loans with significant divergence between FICO and VantageScore';