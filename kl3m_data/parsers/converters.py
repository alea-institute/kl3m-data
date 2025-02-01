"""
Basic converters for data types like JSON, XML, YAML, and CSV.
"""

# imports
import json

# packages
import lxml.etree
import yaml


def etree_to_dict(
    element: lxml.etree.Element,
) -> dict:
    """
    Convert an lxml etree to a dictionary.

    Args:
        element (lxml.etree.Element): Element.

    Returns:
        dict: Dictionary.
    """
    # initialize the dictionary with the root tag name
    root_tag = element.tag
    d: dict[str, list | dict] = {}

    # add all keys for children; create a dict or list as needed
    for child in element.iterchildren():
        # key for child as always string
        tag_name = str(child.tag)

        # check if this is a leaf/terminal node with text
        if len(child) == 0:
            if tag_name not in d:
                d[tag_name] = child.text
            else:
                if not isinstance(d[tag_name], list):
                    d[tag_name] = [d[tag_name]]
                d[tag_name].append(child.text)
        else:
            if tag_name not in d:
                d[tag_name] = etree_to_dict(child)
            else:
                if not isinstance(d[tag_name], list):
                    d[tag_name] = [d[tag_name]]
                d[tag_name].append(etree_to_dict(child))

    return {root_tag: d}


def json_to_yaml(json_input: dict) -> str:
    """
    Convert a JSON input to a YAML string.

    Args:
        json_input (dict): JSON input.

    Returns:
        str: YAML string.
    """
    # convert to yaml
    return yaml.dump(json_input)


def etree_to_json(
    element: lxml.etree.Element,
) -> str:
    """
    Convert an lxml etree to a JSON string.

    Args:
        element (lxml.etree.Element): Element.

    Returns:
        str: JSON string.
    """
    return json.dumps(etree_to_dict(element), indent=2, default=str)


def json_to_etree(
    json_input: dict,
) -> lxml.etree.Element:
    """
    Convert a JSON input to an lxml etree.

    Args:
        json_input (dict): JSON input.

    Returns:
        lxml.etree.Element: Element.
    """
    # create the root element
    root = lxml.etree.Element("root")

    # add the children
    for key, value in json_input.items():
        # create the child element
        child = lxml.etree.Element(key)

        # add the value
        if isinstance(value, dict):
            child = json_to_etree(value)
        else:
            child.text = str(value)

        # append the child to the root
        root.append(child)

    return root


def json_to_xml(
    json_input: dict,
) -> str:
    """
    Convert a JSON input to an XML string.

    Args:
        json_input (dict): JSON input.

    Returns:
        str: XML string.
    """
    return lxml.etree.tostring(json_to_etree(json_input), pretty_print=True).decode()


def etree_to_yaml(
    element: lxml.etree.Element,
) -> str:
    """
    Convert an lxml etree to a YAML string.

    Args:
        element (lxml.etree.Element): Element.

    Returns:
        str: YAML string.
    """
    return yaml.dump(etree_to_dict(element))
