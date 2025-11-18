flowchart TD

    A[Inputs<br>• SQL Database<br>• JSON Config (ProfileID)<br>• DataFrames / Dictionaries] --> B

    B[Data Extraction Layer<br><br>• Run SQL queries<br>• Fetch groups, codes, products<br>• Load into DataFrames] --> C

    C[Data Transformation Layer<br><br>• Clean & normalize data<br>• Map 0/1 → XML values<br>• Build product dictionaries<br>• Summarize ranges] --> D

    D[XML Fragment Generation<br>(convert2XML)<br><br>• Dict/DF → XML<br>• Clean unwanted tags<br>• Output element or file] --> E

    E[XML Tree Construction<br><br>insert_xmltree()<br>append_xmltree()<br><br>• Insert child nodes<br>• Build nested structures<br>• Append intermediate XML] --> F

    F[XML Wrapping<br>(node_wrapper)<br><br>• Surround blocks with parent tag<br>• Finalize schema tree] --> G

    G[Final Output XML<br><br>• Fully assembled<br>• Schema-agnostic<br>• Ready for downstream systems]
