namespace MQT.API;

public class ProductOrdered
{
    public int ProductId { get; set; }
    public string ProductName { get; set; }
    public string Observations { get; set; }
    public uint Quantity { get; set; }
}