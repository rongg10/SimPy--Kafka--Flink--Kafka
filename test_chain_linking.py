#!/usr/bin/env python3
"""
Test script to verify chain linking matching rules.
Tests the matching logic without requiring Flink/Kafka.
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "flink-jobs"))
from linking_utils import WatermarkMatcher


def test_matching_rules():
    """Test the matching rules for parent-child relationships."""
    
    # Test case 1: Linear chain (from requirements example 1)
    print("Test Case 1: Linear Chain")
    print("=" * 50)
    
    messages = [
        {"id": "id1", "src_ip": "10.0.0.1", "dst_ip": "10.0.0.2", "start_at_ms": 1000, "end_at_ms": 1500},
        {"id": "id2", "src_ip": "10.0.0.2", "dst_ip": "10.0.0.3", "start_at_ms": 1100, "end_at_ms": 1300},
        {"id": "id3", "src_ip": "10.0.0.3", "dst_ip": "10.0.0.4", "start_at_ms": 1120, "end_at_ms": 1220},
    ]
    
    # Initialize parents and children
    for msg in messages:
        msg["parents"] = []
        msg["children"] = []
    
    # Apply matching rules
    for i, parent in enumerate(messages):
        for j, child in enumerate(messages):
            if i == j:
                continue
            if (parent["dst_ip"] == child["src_ip"] and
                parent["start_at_ms"] <= child["start_at_ms"] and
                parent["end_at_ms"] >= child["end_at_ms"]):
                if child["id"] not in parent["children"]:
                    parent["children"].append(child["id"])
                if parent["id"] not in child["parents"]:
                    child["parents"].append(parent["id"])
    
    # Verify results
    assert messages[0]["children"] == ["id2"], f"Expected id1.children == ['id2'], got {messages[0]['children']}"
    assert messages[0]["parents"] == [], f"Expected id1.parents == [], got {messages[0]['parents']}"
    assert messages[1]["children"] == ["id3"], f"Expected id2.children == ['id3'], got {messages[1]['children']}"
    assert messages[1]["parents"] == ["id1"], f"Expected id2.parents == ['id1'], got {messages[1]['parents']}"
    assert messages[2]["children"] == [], f"Expected id3.children == [], got {messages[2]['children']}"
    assert messages[2]["parents"] == ["id2"], f"Expected id3.parents == ['id2'], got {messages[2]['parents']}"
    
    print("✓ Linear chain test passed!")
    for msg in messages:
        print(f"  {msg['id']}: parents={msg['parents']}, children={msg['children']}")
    
    # Test case 2: Concurrent branches (from requirements example 2)
    print("\nTest Case 2: Concurrent Branches")
    print("=" * 50)
    
    messages2 = [
        {"id": "id1", "src_ip": "10.0.0.1", "dst_ip": "10.0.0.2", "start_at_ms": 1000, "end_at_ms": 1500},
        {"id": "id2", "src_ip": "10.0.0.2", "dst_ip": "10.0.0.3", "start_at_ms": 1100, "end_at_ms": 1300},
        {"id": "id3", "src_ip": "10.0.0.2", "dst_ip": "10.0.0.4", "start_at_ms": 1200, "end_at_ms": 1400},
    ]
    
    # Initialize parents and children
    for msg in messages2:
        msg["parents"] = []
        msg["children"] = []
    
    # Apply matching rules
    for i, parent in enumerate(messages2):
        for j, child in enumerate(messages2):
            if i == j:
                continue
            if (parent["dst_ip"] == child["src_ip"] and
                parent["start_at_ms"] <= child["start_at_ms"] and
                parent["end_at_ms"] >= child["end_at_ms"]):
                if child["id"] not in parent["children"]:
                    parent["children"].append(child["id"])
                if parent["id"] not in child["parents"]:
                    child["parents"].append(parent["id"])
    
    # Verify results
    assert messages2[0]["children"] == ["id2", "id3"], f"Expected id1.children == ['id2', 'id3'], got {messages2[0]['children']}"
    assert messages2[1]["parents"] == ["id1"], f"Expected id2.parents == ['id1'], got {messages2[1]['parents']}"
    assert messages2[2]["parents"] == ["id1"], f"Expected id3.parents == ['id1'], got {messages2[2]['parents']}"
    
    print("✓ Concurrent branches test passed!")
    for msg in messages2:
        print(f"  {msg['id']}: parents={msg['parents']}, children={msg['children']}")
    
    # Test case 3: Multiple parents (from requirements example 3)
    print("\nTest Case 3: Multiple Parents")
    print("=" * 50)
    
    messages3 = [
        {"id": "id1", "src_ip": "10.0.0.1", "dst_ip": "10.0.0.3", "start_at_ms": 1000, "end_at_ms": 1400},
        {"id": "id2", "src_ip": "10.0.0.2", "dst_ip": "10.0.0.3", "start_at_ms": 1050, "end_at_ms": 1400},
        {"id": "id3", "src_ip": "10.0.0.3", "dst_ip": "10.0.0.4", "start_at_ms": 1200, "end_at_ms": 1300},
    ]
    
    # Initialize parents and children
    for msg in messages3:
        msg["parents"] = []
        msg["children"] = []
    
    # Apply matching rules
    for i, parent in enumerate(messages3):
        for j, child in enumerate(messages3):
            if i == j:
                continue
            if (parent["dst_ip"] == child["src_ip"] and
                parent["start_at_ms"] <= child["start_at_ms"] and
                parent["end_at_ms"] >= child["end_at_ms"]):
                if child["id"] not in parent["children"]:
                    parent["children"].append(child["id"])
                if parent["id"] not in child["parents"]:
                    child["parents"].append(parent["id"])
    
    # Verify results
    assert messages3[0]["children"] == ["id3"], f"Expected id1.children == ['id3'], got {messages3[0]['children']}"
    assert messages3[1]["children"] == ["id3"], f"Expected id2.children == ['id3'], got {messages3[1]['children']}"
    assert set(messages3[2]["parents"]) == {"id1", "id2"}, f"Expected id3.parents == ['id1', 'id2'], got {messages3[2]['parents']}"
    
    print("✓ Multiple parents test passed!")
    for msg in messages3:
        print(f"  {msg['id']}: parents={msg['parents']}, children={msg['children']}")
    
    print("\n" + "=" * 50)
    print("All tests passed! ✓")
    print("=" * 50)


def _make_message(msg_id, src_ip, dst_ip, start_at_ms, end_at_ms):
    return {
        "id": msg_id,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "start_at_ms": start_at_ms,
        "end_at_ms": end_at_ms,
        "parents": [],
        "children": [],
    }


def test_watermark_buffering():
    """Test watermark buffering for reciprocal matching."""
    matcher = WatermarkMatcher(max_out_of_order_ms=0)

    msg1 = _make_message("id1", "10.0.0.1", "10.0.0.2", 1000, 2000)
    msg2 = _make_message("id2", "10.0.0.2", "10.0.0.3", 1100, 1500)
    msg3 = _make_message("id3", "10.0.0.4", "10.0.0.5", 3000, 3100)

    assert matcher.process_message(msg1) == []
    assert matcher.process_message(msg2) == []

    emitted = matcher.process_message(msg3)
    emitted_ids = {msg["id"] for msg in emitted}
    assert emitted_ids == {"id1", "id2"}, f"Expected id1/id2 emitted, got {emitted_ids}"

    emitted_map = {msg["id"]: msg for msg in emitted}
    assert emitted_map["id1"]["children"] == ["id2"], emitted_map["id1"]
    assert emitted_map["id2"]["parents"] == ["id1"], emitted_map["id2"]

    flushed = matcher.emit_up_to(999999)
    assert len(flushed) == 1 and flushed[0]["id"] == "id3", flushed


def test_watermark_out_of_order():
    """Test out-of-order handling with a watermark delay."""
    matcher = WatermarkMatcher(max_out_of_order_ms=200)

    child_first = _make_message("id2", "10.0.0.2", "10.0.0.3", 1100, 1500)
    parent_late = _make_message("id1", "10.0.0.1", "10.0.0.2", 900, 2000)
    progress = _make_message("id3", "10.0.0.9", "10.0.0.10", 2300, 2350)

    assert matcher.process_message(child_first) == []
    assert matcher.process_message(parent_late) == []

    emitted = matcher.process_message(progress)
    emitted_map = {msg["id"]: msg for msg in emitted}
    assert "id1" in emitted_map and "id2" in emitted_map, emitted_map
    assert emitted_map["id1"]["children"] == ["id2"], emitted_map["id1"]
    assert emitted_map["id2"]["parents"] == ["id1"], emitted_map["id2"]


if __name__ == "__main__":
    test_matching_rules()
    test_watermark_buffering()
    test_watermark_out_of_order()
