"""
Pure-Python linking helpers for SimPy chain matching and watermark buffering.
These are shared by tests and the Flink job.
"""
from typing import Dict, List, Optional


def ensure_link_fields(message: Dict) -> Dict:
    """Ensure parents/children fields exist."""
    if "parents" not in message:
        message["parents"] = []
    if "children" not in message:
        message["children"] = []
    return message


def is_parent(parent: Dict, child: Dict) -> bool:
    """Return True when parent/child matching rules are satisfied."""
    return (
        parent["dst_ip"] == child["src_ip"]
        and parent["start_at_ms"] <= child["start_at_ms"]
        and parent["end_at_ms"] >= child["end_at_ms"]
    )


def link_parent_child(parent: Dict, child: Dict) -> bool:
    """Link parent/child ids on both records. Returns True if any change."""
    changed = False
    if child["id"] not in parent["children"]:
        parent["children"].append(child["id"])
        changed = True
    if parent["id"] not in child["parents"]:
        child["parents"].append(parent["id"])
        changed = True
    return changed


def link_if_match(parent: Dict, child: Dict) -> bool:
    """Link parent/child if they match. Returns True if any change."""
    if is_parent(parent, child):
        return link_parent_child(parent, child)
    return False


class WatermarkMatcher:
    """
    Buffer messages until watermark passes their end_at_ms.
    Watermark uses max start_at_ms seen minus max_out_of_order_ms.
    """

    def __init__(self, max_out_of_order_ms: int) -> None:
        self.max_out_of_order_ms = max_out_of_order_ms
        self.max_start_ms: Optional[int] = None
        self.messages: Dict[str, Dict] = {}

    def process_message(self, message: Dict) -> List[Dict]:
        """Process a message and emit any records ready for the watermark."""
        ensure_link_fields(message)
        for cached in self.messages.values():
            link_if_match(cached, message)
            link_if_match(message, cached)
        self.messages[message["id"]] = message
        self._update_watermark(message["start_at_ms"])
        return self.emit_ready()

    def _update_watermark(self, start_at_ms: int) -> None:
        if self.max_start_ms is None or start_at_ms > self.max_start_ms:
            self.max_start_ms = start_at_ms

    def current_watermark(self) -> Optional[int]:
        if self.max_start_ms is None:
            return None
        return self.max_start_ms - self.max_out_of_order_ms

    def emit_ready(self) -> List[Dict]:
        watermark = self.current_watermark()
        if watermark is None:
            return []
        return self.emit_up_to(watermark)

    def emit_up_to(self, watermark_ms: int) -> List[Dict]:
        ready: List[Dict] = []
        for msg_id, msg in list(self.messages.items()):
            if msg["end_at_ms"] <= watermark_ms:
                ready.append(msg)
                del self.messages[msg_id]
        return ready
