// queries from looker dashboard
// Risk and Fraud Loss by Month (BPS)
SELECT
	TO_CHAR(DATE_TRUNC('month', risk_losses.LOSS_MONTH ), 'YYYY-MM') AS "risk_losses.loss_month",
	((COALESCE(SUM(risk_losses.DISPUTE_NET_LOSS ), 0)) + (COALESCE(SUM(risk_losses.DEBIT_CARD_NET_LOSS ), 0)) + (COALESCE(SUM(risk_losses.ACH_NET_LOSS ), 0)) + (COALESCE(SUM(risk_losses.MOBILE_CHECK_DEPOSIT_NET_LOSS ), 0)))  / nullif((COALESCE(SUM(risk_losses.TRANSACTION_AMOUNT ), 0)),0) * 100 * 100  AS "risk_losses.total_losses_bps"
FROM chime.finance.risk_losses_view  AS risk_losses

WHERE
	(((risk_losses.LOSS_MONTH ) >= ((DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))) AND (risk_losses.LOSS_MONTH ) < ((DATEADD('month', 13, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))))))
GROUP BY DATE_TRUNC('month', risk_losses.LOSS_MONTH )
ORDER BY 1 DESC;

// Risk and Fraud Loss by Type
SELECT
	TO_CHAR(DATE_TRUNC('month', risk_losses.LOSS_MONTH ), 'YYYY-MM') AS "risk_losses.loss_month",
	(COALESCE(SUM(risk_losses.DISPUTE_CREDITS_GIVEN ), 0)) / nullif((COALESCE(SUM(risk_losses.TRANSACTION_AMOUNT ), 0)),0) * 100 * 100  AS "risk_losses.dispute_credits_given_bps",
	(COALESCE(SUM(risk_losses.DISPUTE_NEGATIVE_BALANCE ), 0))  / nullif((COALESCE(SUM(risk_losses.TRANSACTION_AMOUNT ), 0)),0) * 100 * 100  AS "risk_losses.dispute_negative_balance_amount_bps",
	(COALESCE(SUM(risk_losses.DEBIT_CARD_NET_LOSS ), 0))  / nullif((COALESCE(SUM(risk_losses.TRANSACTION_AMOUNT ), 0)),0) * 100 * 100  AS "risk_losses.debit_card_net_loss_bps",
	(COALESCE(SUM(risk_losses.ACH_NET_LOSS ), 0))  / nullif((COALESCE(SUM(risk_losses.TRANSACTION_AMOUNT ), 0)),0) * 100 * 100  AS "risk_losses.ach_loss_bps",
	(COALESCE(SUM(risk_losses.MOBILE_CHECK_DEPOSIT_NET_LOSS ), 0))  / nullif((COALESCE(SUM(risk_losses.TRANSACTION_AMOUNT ), 0)),0) * 100 * 100  AS "risk_losses.mobile_check_deposit_loss_bps"
FROM chime.finance.risk_losses_view  AS risk_losses

WHERE
	(((risk_losses.LOSS_MONTH ) >= ((DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))) AND (risk_losses.LOSS_MONTH ) < ((DATEADD('month', 13, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))))))
GROUP BY DATE_TRUNC('month', risk_losses.LOSS_MONTH )
ORDER BY 1 DESC;

// Risk and Fraud Loss by Month and by Type (Dollars)
SELECT
	TO_CHAR(DATE_TRUNC('month', risk_losses.LOSS_MONTH ), 'YYYY-MM') AS "risk_losses.loss_month",
	COALESCE(SUM(risk_losses.DISPUTE_CREDITS_GIVEN ), 0) AS "risk_losses.dispute_credits_given",
	COALESCE(SUM(risk_losses.DISPUTE_NEGATIVE_BALANCE ), 0) AS "risk_losses.dispute_negative_balance",
	COALESCE(SUM(risk_losses.DEBIT_CARD_NET_LOSS ), 0) AS "risk_losses.debit_card_net_loss",
	COALESCE(SUM(risk_losses.ACH_NET_LOSS ), 0) AS "risk_losses.ach_net_loss",
	COALESCE(SUM(risk_losses.MOBILE_CHECK_DEPOSIT_NET_LOSS ), 0) AS "risk_losses.mobile_check_deposit_net_loss"
FROM chime.finance.risk_losses_view  AS risk_losses

WHERE
	(((risk_losses.LOSS_MONTH ) >= ((DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))) AND (risk_losses.LOSS_MONTH ) < ((DATEADD('month', 13, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))))))
GROUP BY DATE_TRUNC('month', risk_losses.LOSS_MONTH )
ORDER BY 1 DESC; 

// MCD Loss by MCD Volume
SELECT
	TO_CHAR(DATE_TRUNC('month', risk_losses.LOSS_MONTH ), 'YYYY-MM') AS "risk_losses.loss_month",
	COALESCE(SUM(risk_losses.CHECK_DEPOSIT_VOLUME ), 0) AS "risk_losses.mobile_check_deposit_volume",
    COALESCE(SUM(risk_losses.MOBILE_CHECK_DEPOSIT_NET_LOSS), 0) AS "risk_losses.mobile_check_deposit_loss_mcd",
	(COALESCE(SUM(risk_losses.MOBILE_CHECK_DEPOSIT_NET_LOSS), 0))  / nullif((COALESCE(SUM(risk_losses.CHECK_DEPOSIT_VOLUME ), 0)),0) * 100 * 100  AS "risk_losses.mobile_check_deposit_loss_mcd_bps"
FROM chime.finance.risk_losses_view AS risk_losses

WHERE
	(((risk_losses.LOSS_MONTH ) >= ((DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))) AND (risk_losses.LOSS_MONTH ) < ((DATEADD('month', 13, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))))))
GROUP BY DATE_TRUNC('month', risk_losses.LOSS_MONTH )
ORDER BY 1;

// ACH Loss by ACH Volume
SELECT
	TO_CHAR(DATE_TRUNC('month', risk_losses.LOSS_MONTH ), 'YYYY-MM') AS "risk_losses.loss_month",
	COALESCE(SUM(risk_losses.ACH_VOLUME ), 0) AS "risk_losses.ach_volume",
	(COALESCE(SUM(risk_losses.ACH_NET_LOSS ), 0))  / nullif((COALESCE(SUM(risk_losses.ACH_VOLUME ), 0)),0) * 100 * 100  AS "risk_losses.ach_loss_ach_bps"
FROM chime.finance.risk_losses_view  AS risk_losses

WHERE
	(((risk_losses.LOSS_MONTH ) >= ((DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))) AND (risk_losses.LOSS_MONTH ) < ((DATEADD('month', 13, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))))))
GROUP BY DATE_TRUNC('month', risk_losses.LOSS_MONTH )
ORDER BY 1;

//Debit Card Loss by Debit Card Volume
SELECT
	TO_CHAR(DATE_TRUNC('month', risk_losses.LOSS_MONTH ), 'YYYY-MM') AS "risk_losses.loss_month",
	COALESCE(SUM(risk_losses.DEBIT_CARD_VOLUME ), 0) AS "risk_losses.debit_card_volume",
	(COALESCE(SUM(risk_losses.DEBIT_CARD_NET_LOSS ), 0))  / nullif((COALESCE(SUM(risk_losses.DEBIT_CARD_VOLUME ), 0)),0) * 100 * 100  AS "risk_losses.debit_card_net_loss_dc_bps"
FROM chime.finance.risk_losses_view  AS risk_losses

WHERE
	(((risk_losses.LOSS_MONTH ) >= ((DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))) AND (risk_losses.LOSS_MONTH ) < ((DATEADD('month', 13, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())))))))
GROUP BY DATE_TRUNC('month', risk_losses.LOSS_MONTH )
ORDER BY 1
