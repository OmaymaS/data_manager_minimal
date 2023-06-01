FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY src/$COMPONENT/pipeline ${WORKDIR}/pipeline

COPY src/$COMPONENT/spec/python_command_spec.json ${WORKDIR}/python_command_spec.json

ENV DATAFLOW_PYTHON_COMMAND_SPEC ${WORKDIR}/python_command_spec.json


COPY src/$COMPONENT/setup.py ${WORKDIR}/setup.py
COPY src/$COMPONENT/main.py ${WORKDIR}/main.py
COPY src/$COMPONENT/requirements.txt ${WORKDIR}/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r ${WORKDIR}/requirements.txt
RUN pip install -e ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
