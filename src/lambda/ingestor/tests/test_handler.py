"""Verify the master data job is isolated from the retired order poller."""
import json
from unittest.mock import Mock, patch

import handler


def test_legacy_order_schedule_is_ignored():
    with patch.object(handler, "make_client") as make_client:
        result = handler.lambda_handler({"sources": ["orders", "shipping_status"]}, None)
    make_client.assert_not_called()
    assert result["written"] == {}


def test_master_data_refresh_archives_only_master_data():
    records = [{"entity": "province"}, {"entity": "district"}, {"entity": "ward"}]
    api = Mock()
    api.fetch_master_data.return_value = records
    s3 = Mock()
    with (
        patch.object(handler, "make_client", return_value=api),
        patch.dict("os.environ", {"GHN_WARD_DISTRICTS": "101, 202"}),
        patch.dict("sys.modules", {"boto3": Mock(client=Mock(return_value=s3))}),
    ):
        result = handler.lambda_handler({"sources": ["master_data"]}, None)

    api.fetch_master_data.assert_called_once_with(ward_district_ids=[101, 202])
    api.fetch_orders.assert_not_called()
    s3.put_object.assert_called_once()
    write = s3.put_object.call_args.kwargs
    assert write["Key"].startswith("raw/master_data/")
    assert json.loads(write["Body"])["records"] == records
    assert result["written"]["master_data"]["count"] == len(records)
