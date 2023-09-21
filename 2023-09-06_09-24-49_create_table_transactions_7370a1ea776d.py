"""create table transactions

Revision ID: 7370a1ea776d
Revises: 
Create Date: 2023-09-06 09:24:49.722811

"""
from alembic import op
import sqlalchemy as sa

from clickhouse_sqlalchemy import engines
from clickhouse_sqlalchemy import types


# revision identifiers, used by Alembic.
revision = '7370a1ea776d'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("affiliate.click_id", types.String),
        sa.Column("affiliate.id", types.Int32),
        sa.Column("affiliate.name", types.String),
        sa.Column("affiliate.offer_id", types.Int16),
        sa.Column("affiliate.user_age", types.Int32),
        sa.Column("affiliate.user_age_type", types.String),
        sa.Column("affiliate.user_type", types.String),

        sa.Column("all_aff_clicks", types.String),
        sa.Column("billing", types.String),
        sa.Column("cb_transaction_id", types.Int32),
        sa.Column("currency", types.String),

        sa.Column("customer.browser.family", types.String),
        sa.Column("customer.browser.version", types.String),
        sa.Column("customer.country", types.String),
        sa.Column("customer.credit_card.bank", types.String),
        sa.Column("customer.credit_card.bin", types.String),
        sa.Column("customer.credit_card.country", types.String),
        sa.Column("customer.credit_card.network", types.String),
        sa.Column("customer.credit_card.subtype", types.String),
        sa.Column("customer.credit_card.type", types.String),
        sa.Column("customer.device.brand", types.String),
        sa.Column("customer.device.family", types.String),
        sa.Column("customer.device.model", types.String),
        sa.Column("customer.device.type", types.String),
        sa.Column("customer.email", types.String),
        sa.Column("customer.geoip.asn", types.String),
        sa.Column("customer.geoip.city", types.String),
        sa.Column("customer.geoip.continent", types.String),
        sa.Column("customer.geoip.country", types.String),
        sa.Column("customer.geoip.organization", types.String),
        sa.Column("customer.geoip.region", types.String),
        sa.Column("customer.id", types.Int32),
        sa.Column("customer.ip_address", types.String),
        sa.Column("customer.is_bot", types.Boolean),
        sa.Column("customer.logged_in", types.Boolean),
        sa.Column("customer.os.family", types.String),
        sa.Column("customer.os.version", types.String),
        sa.Column("customer.session_id", types.String),
        sa.Column("customer.user_agent", types.String),
        sa.Column("customer.zip_code", types.String),

        sa.Column("id", types.Int32),

        sa.Column("initial_transaction.affiliate.name", types.String),
        sa.Column("initial_transaction.days_from", types.Int32),
        sa.Column("initial_transaction.id", types.Int32),
        sa.Column("initial_transaction.timestamp", types.DateTime),

        sa.Column("last_aff_click.affiliate.id", types.Int32),
        sa.Column("last_aff_click.campaign", types.String),
        sa.Column("last_aff_click.days_from", types.Int32),
        sa.Column("last_aff_click.query_params.click_id", types.Int32),
        sa.Column("last_aff_click.query_params.p1", types.String),
        sa.Column("last_aff_click.query_params.p2", types.String),
        sa.Column("last_aff_click.query_params.p3", types.String),
        sa.Column("last_aff_click.query_params.p4", types.String),
        sa.Column("last_aff_click.query_params.p5", types.String),
        sa.Column("last_aff_click.query_params.p6", types.String),
        sa.Column("last_aff_click.query_params.p7", types.String),
        sa.Column("last_aff_click.query_params.p8", types.String),
        sa.Column("last_aff_click.query_params.src", types.String),
        sa.Column("last_aff_click.query_params.sub", types.String),
        sa.Column("last_aff_click.query_params.z", types.String),
        sa.Column("last_aff_click.rtb.domain", types.String),
        sa.Column("last_aff_click.source", types.String),

        sa.Column("meta.origin_processed_at", types.Int64),
        sa.Column("meta.processed_at", types.Int64),
        sa.Column("meta.source", types.String),

        sa.Column("misc.affiliate.initial", types.Boolean),
        sa.Column("misc.affiliate.within_same_session", types.Boolean),
        sa.Column("misc.clips_count", types.Int32),
        sa.Column("misc.happened_from", types.String),
        sa.Column("misc.order_number", types.Int32),

        sa.Column("oneclick", types.Boolean),
        sa.Column("payment_method", types.String),

        sa.Column("previous_transaction.days_from", types.Int32),
        sa.Column("previous_transaction.id", types.UInt32),
        sa.Column("previous_transaction.timestamp", types.DateTime),

        sa.Column("price.usd", types.Float32),
        sa.Column("price.usd_with_taxes", types.Float32),
        sa.Column("studio.payout", types.Float32),
        sa.Column("taxes.usd", types.Float32),
        sa.Column("test", types.Boolean),
        sa.Column("test_group", types.String),
        sa.Column("timestamp", types.DateTime),
        sa.Column("_id", types.String),

        sa.Column("sign", types.Int8, server_default=sa.literal(1)),
        engines.CollapsingMergeTree(sign_col="sign", partition_by=sa.text("toYYYYMM(timestamp)"),
                                    order_by=("timestamp", "id"))
    )


def downgrade() -> None:
    op.drop_table("transactions")
