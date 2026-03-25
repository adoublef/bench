package order

import "strconv"

type Order struct {
	Duration     int     `json:"duration"`
	IsBuyOrder   bool    `json:"is_buy_order"`
	Issued       string  `json:"issued"`
	LocationID   int     `json:"location_id"`
	MinVolume    int     `json:"min_volume"`
	OrderID      int     `json:"order_id"`
	Price        float64 `json:"price"`
	Range        string  `json:"range"`
	SystemID     int     `json:"system_id"`
	TypeID       int     `json:"type_id"`
	VolumeRemain int     `json:"volume_remain"`
	VolumeTotal  int     `json:"volume_total"`
}

func (o Order) Record() [12]string {
	return [12]string{strconv.Itoa(o.Duration),
		strconv.FormatBool(o.IsBuyOrder),
		o.Issued,
		strconv.Itoa(o.LocationID),
		strconv.Itoa(o.MinVolume),
		strconv.Itoa(o.OrderID),
		strconv.FormatFloat(o.Price, 'f', -1, 64),
		o.Range,
		strconv.Itoa(o.SystemID),
		strconv.Itoa(o.TypeID),
		strconv.Itoa(o.VolumeRemain),
		strconv.Itoa(o.VolumeTotal),
	}
}
