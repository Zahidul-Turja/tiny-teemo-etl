from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.core.constants import DataType, DateFormat, DateTimeFormat
from app.models.schemas import DataTypeInfo

router = APIRouter()


@router.get("/data-types", summary="List all supported data types")
def get_data_types() -> JSONResponse:
    data_types = [
        DataTypeInfo(
            type_id=DataType.INTEGER.value,
            display_name="Integer",
            description="Whole numbers without decimals",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.BIGINT.value,
            display_name="Big Integer",
            description="Large whole numbers (> 2.1 billion)",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.FLOAT.value,
            display_name="Float",
            description="Decimal numbers (double precision)",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.DECIMAL.value,
            display_name="Decimal",
            description="Fixed-precision decimal numbers",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.STRING.value,
            display_name="String (VARCHAR)",
            description="Text up to 255 characters",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.TEXT.value,
            display_name="Text",
            description="Long-form text content",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.BOOLEAN.value,
            display_name="Boolean",
            description="True/False values",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.JSON.value,
            display_name="JSON",
            description="JSON stored as text",
            requires_format=False,
        ),
        DataTypeInfo(
            type_id=DataType.DATE.value,
            display_name="Date",
            description="Date without time",
            requires_format=True,
            available_formats=[f.value for f in DateFormat],
        ),
        DataTypeInfo(
            type_id=DataType.DATETIME.value,
            display_name="DateTime",
            description="Date with time",
            requires_format=True,
            available_formats=[f.value for f in DateTimeFormat],
        ),
        DataTypeInfo(
            type_id=DataType.TIMESTAMP.value,
            display_name="Timestamp",
            description="Date and time with timezone",
            requires_format=True,
            available_formats=[f.value for f in DateTimeFormat],
        ),
    ]

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "success": True,
            "data": {
                "data_types": [dt.model_dump() for dt in data_types],
            },
        },
    )


@router.get("/date-formats", summary="List all supported date formats with examples")
def get_date_formats() -> JSONResponse:
    formats = [
        {"format": f.value, "example": _date_example(f.value)} for f in DateFormat
    ]
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"success": True, "data": {"formats": formats}},
    )


@router.get(
    "/datetime-formats", summary="List all supported datetime formats with examples"
)
def get_datetime_formats() -> JSONResponse:
    formats = [
        {"format": f.value, "example": _datetime_example(f.value)}
        for f in DateTimeFormat
    ]
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"success": True, "data": {"formats": formats}},
    )


# ── helpers ──────────────────────────────────────────────────────────────────


def _date_example(fmt: str) -> str:
    return {
        "YYYY-MM-DD": "2024-02-12",
        "YY-MM-DD": "24-02-12",
        "DD-MM-YYYY": "12-02-2024",
        "DD-MM-YY": "12-02-24",
        "YYYY/MM/DD": "2024/02/12",
        "YY/MM/DD": "24/02/12",
        "DD/MM/YYYY": "12/02/2024",
        "DD/MM/YY": "12/02/24",
        "MMM DD, YYYY": "Feb 12, 2024",
        "MMMM DD, YYYY": "February 12, 2024",
    }.get(fmt, "2024-02-12")


def _datetime_example(fmt: str) -> str:
    return {
        "YYYY-MM-DD HH:MM:SS": "2024-02-12 14:30:00",
        "DD-MM-YYYY HH:MM:SS": "12-02-2024 14:30:00",
        "MM-DD-YYYY HH:MM:SS": "02-12-2024 14:30:00",
        "YYYY-MM-DDTHH:MM:SS": "2024-02-12T14:30:00",
        "ISO8601": "2024-02-12T14:30:00Z",
    }.get(fmt, "2024-02-12 14:30:00")
