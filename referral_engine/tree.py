from __future__ import annotations

from typing import TYPE_CHECKING, List

from referral_engine.models import TreeNode

if TYPE_CHECKING:
    from referral_engine.adapters.base import BaseAdapter


async def check_cycle(
    adapter: "BaseAdapter",
    user_id: int,
    referrer_id: int,
    max_depth: int,
) -> bool:
    """Return True if linking user_id → referrer_id would create a cycle."""
    if user_id == referrer_id:
        return True
    current = referrer_id
    visited = {user_id}
    for _ in range(max_depth):
        if current in visited:
            return True
        visited.add(current)
        parent = await adapter.get_parent(current)
        if parent is None:
            return False
        current = parent.referrer_id
    return False


async def get_tree_up(
    adapter: "BaseAdapter",
    user_id: int,
    max_depth: int,
) -> List[TreeNode]:
    """Flat ancestor list, sorted by level (1 = direct parent first)."""
    links = await adapter.get_chain_up(user_id, max_depth)
    return [
        TreeNode(user_id=link.referrer_id, level=link.level)
        for link in sorted(links, key=lambda l: l.level)
    ]


async def get_tree_down(
    adapter: "BaseAdapter",
    user_id: int,
    max_depth: int,
) -> TreeNode:
    """Nested TreeNode subtree rooted at user_id."""
    root = TreeNode(user_id=user_id, level=0)
    await _fill(adapter, root, max_depth)
    return root


async def _fill(
    adapter: "BaseAdapter",
    node: TreeNode,
    remaining: int,
) -> None:
    if remaining <= 0:
        return
    children = await adapter.get_children(node.user_id, level=1)
    for link in children:
        child = TreeNode(user_id=link.user_id, level=node.level + 1)
        node.children.append(child)
        await _fill(adapter, child, remaining - 1)