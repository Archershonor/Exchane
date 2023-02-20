from sqlalchemy_test import Column
from sqlalchemy_test import create_engine
from sqlalchemy_test import ForeignKey
from sqlalchemy_test import Integer
from sqlalchemy_test import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import attribute_keyed_dict


Base = declarative_base()


class Curency(Base):
    __tablename__ = "curency"
    id = Column(Integer, primary_key=True)
    code = Column(String(5), nullable=False)
    name = Column(String(100))

    currency_values = relationship(
        "CurrencyValues", cascade="all, delete-orphan", backref="order"
    )
    
    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent

    def __repr__(self):
        return "TreeNode(name=%r, id=%r, parent_id=%r)" % (
            self.name,
            self.id,
            self.parent_id,
        )

    def dump(self, _indent=0):
        return (
            "   " * _indent
            + repr(self)
            + "\n"
            + "".join([c.dump(_indent + 1) for c in self.children.values()])
        )


if __name__ == "__main__":
    engine = create_engine("sqlite://", echo=True)

    def msg(msg, *args):
        msg = msg % args
        print("\n\n\n" + "-" * len(msg.split("\n")[0]))
        print(msg)
        print("-" * len(msg.split("\n")[0]))

    msg("Creating Tree Table:")

    Base.metadata.create_all(engine)

    session = Session(engine)

    node = TreeNode("rootnode")
    TreeNode("node1", parent=node)
    TreeNode("node3", parent=node)

    node2 = TreeNode("node2")
    TreeNode("subnode1", parent=node2)
    node.children["node2"] = node2
    TreeNode("subnode2", parent=node.children["node2"])

    msg("Created new tree structure:\n%s", node.dump())

    msg("flush + commit:")

    session.add(node)
    session.commit()

    msg("Tree After Save:\n %s", node.dump())

    TreeNode("node4", parent=node)
    TreeNode("subnode3", parent=node.children["node4"])
    TreeNode("subnode4", parent=node.children["node4"])
    TreeNode("subsubnode1", parent=node.children["node4"].children["subnode3"])

    # remove node1 from the parent, which will trigger a delete
    # via the delete-orphan cascade.
    del node.children["node1"]

    msg("Removed node1.  flush + commit:")
    session.commit()

    msg("Tree after save:\n %s", node.dump())

    msg(
        "Emptying out the session entirely, selecting tree on root, using "
        "eager loading to join four levels deep."
    )
    session.expunge_all()
    node = (
        session.query(TreeNode)
        .options(
            joinedload("children")
            .joinedload("children")
            .joinedload("children")
            .joinedload("children")
        )
        .filter(TreeNode.name == "rootnode")
        .first()
    )

    msg("Full Tree:\n%s", node.dump())

    msg("Marking root node as deleted, flush + commit:")

    session.delete(node)
    session.commit()