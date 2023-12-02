using Microsoft.AspNetCore.Mvc;

namespace MQT.API.Controllers;

[ApiController]
[Route("/api/test")]
public class TestController : ControllerBase
{
    [HttpGet]
    public IActionResult Test() => Ok("Test");
}