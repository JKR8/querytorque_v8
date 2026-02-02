# qt-dax

DAX and Power BI model analysis product for QueryTorque.

## Features

- VPAX file parsing and analysis
- DAX expression validation
- Model quality scoring (Torque Score)
- LLM-powered DAX optimization

## CLI

```bash
qt-dax audit <model.vpax>
qt-dax optimize <model.vpax>
qt-dax connect
qt-dax diff <model_v1.vpax> <model_v2.vpax>
qt-dax validate <orig.dax> <opt.dax>
```

PBIP inputs are supported for audit/optimize (pass the `.pbip` file or the `.SemanticModel` folder).

DSPy optimization with validation (requires Power BI Desktop running):

```bash
qt-dax optimize <model.vpax> --dspy --port <pbi_port>
```
