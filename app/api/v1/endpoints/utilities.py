from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/data-types")
def get_data_types():
    # Replace with database later
    data_types = [
        ("int", "Integer"),
        ("float", "Float"),
        ("str", "String"),
        ("bool", "Boolean"),
        ("date", "YYYY/MM/DD"),
        ("date", "YY/MM/DD"),
        ("date", "DD/MM/YYYY"),
        ("date", "DD/MM/YY"),
        ("date", "YYYY-MM-DD"),
        ("date", "YY-MM-DD"),
        ("date", "DD-MM-YYYY"),
        ("date", "DD-MM-YY"),
        ("date", "MMM DD, YYYY"),
        ("date_time", "DD-MM-YYYY HH:MM:SS"),
    ]

    return JSONResponse(
        {
            "message": "Data types",
            "data": data_types,
        },
        status_code=status.HTTP_200_OK,
    )
