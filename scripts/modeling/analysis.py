def analyze_credit_scores(**kwargs):
    """
    Compare VantageScore 4.0 vs FICO with simple modeling
    """
    import pandas as pd
    import matplotlib.pyplot as plt
    from sqlalchemy import create_engine
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score, roc_curve
    from sklearn.model_selection import train_test_split
    import pickle
    import os
    
    print("\n" + "=" * 80)
    print("Analyzing VantageScore 4.0 vs FICO")
    print("=" * 80)
    
    # Create output directories
    viz_dir = "/opt/airflow/data/outputs/visualizations"
    model_dir = "/opt/airflow/data/outputs/models"
    os.makedirs(viz_dir, exist_ok=True)
    os.makedirs(model_dir, exist_ok=True)
    
    # Connect to database
    db_url = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(db_url)
    
    # Load data
    query = """
    SELECT 
        "Borrower Credit Score at Origination" as fico_score,
        vs4_trimerge,
        "Current Loan Delinquency Status" as delinquency_status
    FROM cirt_vantagescore_merged
    WHERE "Borrower Credit Score at Origination" IS NOT NULL
      AND vs4_trimerge IS NOT NULL
      AND "Current Loan Delinquency Status" IS NOT NULL
    LIMIT 100000
    """
    
    df = pd.read_sql(query, engine)
    print(f"✓ Loaded {len(df):,} loans")
    
    # Create binary target
    df['is_delinquent'] = (df['delinquency_status'] > 0).astype(int)
    
    # Stats
    print(f"\nFICO - Mean: {df['fico_score'].mean():.1f}, Std: {df['fico_score'].std():.1f}")
    print(f"VS4  - Mean: {df['vs4_trimerge'].mean():.1f}, Std: {df['vs4_trimerge'].std():.1f}")
    
    corr = df[['fico_score', 'vs4_trimerge']].corr().iloc[0, 1]
    print(f"Correlation: {corr:.3f}")
    
    # Train models
    print("\nTraining logistic regression models...")
    X_fico = df[['fico_score']].values
    X_vs4 = df[['vs4_trimerge']].values
    y = df['is_delinquent']
    
    X_fico_train, X_fico_test, y_train, y_test = train_test_split(
        X_fico, y, test_size=0.3, random_state=42, stratify=y
    )
    X_vs4_train, X_vs4_test, _, _ = train_test_split(
        X_vs4, y, test_size=0.3, random_state=42, stratify=y
    )
    
    model_fico = LogisticRegression(random_state=42)
    model_fico.fit(X_fico_train, y_train)
    
    model_vs4 = LogisticRegression(random_state=42)
    model_vs4.fit(X_vs4_train, y_train)
    
    # Predictions
    y_proba_fico = model_fico.predict_proba(X_fico_test)[:, 1]
    y_proba_vs4 = model_vs4.predict_proba(X_vs4_test)[:, 1]
    
    auc_fico = roc_auc_score(y_test, y_proba_fico)
    auc_vs4 = roc_auc_score(y_test, y_proba_vs4)
    
    print(f"FICO AUC: {auc_fico:.4f}")
    print(f"VS4 AUC:  {auc_vs4:.4f}")
    
    # Save models
    with open(f"{model_dir}/fico_model.pkl", 'wb') as f:
        pickle.dump(model_fico, f)
    with open(f"{model_dir}/vs4_model.pkl", 'wb') as f:
        pickle.dump(model_vs4, f)
    print(f"✓ Saved models to {model_dir}")
    
    # Create visualizations
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    
    # 1. Correlation scatter
    axes[0].scatter(df['fico_score'], df['vs4_trimerge'], alpha=0.3, s=1)
    axes[0].set_xlabel('FICO Score')
    axes[0].set_ylabel('VantageScore 4.0')
    axes[0].set_title(f'Correlation: {corr:.3f}')
    axes[0].grid(True, alpha=0.3)
    
    # 2. Distributions
    current = df[df['is_delinquent'] == 0]
    delinquent = df[df['is_delinquent'] == 1]
    
    axes[1].hist(current['fico_score'], bins=40, alpha=0.5, label='Current', color='green')
    axes[1].hist(delinquent['fico_score'], bins=40, alpha=0.5, label='Delinquent', color='red')
    axes[1].set_xlabel('FICO Score')
    axes[1].set_ylabel('Frequency')
    axes[1].set_title('FICO Distribution')
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    
    # 3. ROC Curves
    fpr_fico, tpr_fico, _ = roc_curve(y_test, y_proba_fico)
    fpr_vs4, tpr_vs4, _ = roc_curve(y_test, y_proba_vs4)
    
    axes[2].plot(fpr_fico, tpr_fico, label=f'FICO (AUC={auc_fico:.3f})', linewidth=2)
    axes[2].plot(fpr_vs4, tpr_vs4, label=f'VS4 (AUC={auc_vs4:.3f})', linewidth=2)
    axes[2].plot([0, 1], [0, 1], 'k--', alpha=0.3)
    axes[2].set_xlabel('False Positive Rate')
    axes[2].set_ylabel('True Positive Rate')
    axes[2].set_title('ROC Curves')
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    viz_path = f"{viz_dir}/credit_score_analysis.png"
    plt.savefig(viz_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved plot: {viz_path}")
    plt.close()
    
    return {
        'samples': len(df),
        'correlation': float(corr),
        'fico_auc': float(auc_fico),
        'vs4_auc': float(auc_vs4)
    }