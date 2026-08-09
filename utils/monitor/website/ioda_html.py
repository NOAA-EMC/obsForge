class IodaHTML:
    """
    Renders an IodaStructure as HTML.

    Today: renders invariant IODA structure only.
    Future: may optionally overlay file-specific IodaContent.
    """

    def render(self, structure, content=None):
        """
        Render the IODA structure as a collapsible, aligned HTML spec.

        Parameters
        ----------
        structure : IodaStructure
            The invariant IODA structure.
        content : IodaContent | None
            Optional file-specific content (unused for now).
        """

        def is_group_open(name):
            # UI decision only — safe to keep here
            return name in ("MetaData", "ObsValue")

        html = []

        # --- OUTER CONTAINER ---
        html.append("<div class='ioda-structure-container'>")
        html.append("<details>")
        html.append(
            "<summary style='font-size: 1.4em; font-weight: bold; cursor: pointer;'>"
            "IODA Structure"
            "</summary>"
        )
        html.append("<div style='padding: 15px; margin-top: 10px;'>")

        schema = structure.as_dict()

        # --- GLOBAL ATTRIBUTES ---
        if schema.get("global_attributes"):
            html.append("<details>")
            html.append("<summary><strong>Global Attributes</strong></summary>")
            html.append("<table class='structure-table'>")
            for k, v in schema["global_attributes"].items():
                html.append(
                    f"<tr><td><code>{k}</code></td><td>{v}</td></tr>"
                )
            html.append("</table>")
            html.append("</details>")

        # --- DIMENSIONS ---
        if schema.get("dimensions"):
            html.append("<details>")
            html.append("<summary><strong>Dimensions</strong></summary>")
            html.append("<ul>")
            for name, d in schema["dimensions"].items():
                unlimited = " (Unlimited)" if d.get("isunlimited") else ""
                html.append(
                    f"<li><code>{name}</code>: {d.get('size')}{unlimited}</li>"
                )
            html.append("</ul>")
            html.append("</details>")

        # --- GROUPS & VARIABLES ---
        html.append("<h4>Groups & Variables</h4>")

        for group in structure.get_groups():
            open_attr = " open" if is_group_open(group) else ""
            html.append(f"<details{open_attr}>")
            html.append(f"<summary><strong>{group}</strong></summary>")

            html.append("""
            <table class='structure-table' style='width:100%; table-layout:fixed'>
              <thead>
                <tr>
                  <th style='width:25%'>Variable</th>
                  <th style='width:15%'>Type</th>
                  <th style='width:25%'>Dimensions</th>
                  <th style='width:35%'>Attributes</th>
                </tr>
              </thead>
              <tbody>
            """)

            for var in structure.get_vars_in_group(group):
                spec = structure.get_var_spec(group, var)

                attrs = spec.get("attributes", {})
                attr_html = (
                    "<br>".join(
                        f"<i>{k}</i>: {v}" for k, v in attrs.items()
                    )
                    if attrs else "<i>None</i>"
                )

                # --- hydration hooks (unused for now) ---
                html.append(
                    f"<tr data-group='{group}' data-var='{var}'>"
                )
                html.append(f"<td><code>{var}</code></td>")
                html.append(f"<td>{spec.get('dtype')}</td>")
                html.append(
                    f"<td>{', '.join(spec.get('dimensions', []))}</td>"
                )
                html.append(f"<td><small>{attr_html}</small></td>")
                html.append("</tr>")

            html.append("</tbody></table>")
            html.append("</details>")

        # --- CLOSE ---
        html.append("</div></details></div>")

        return "\n".join(html)
