use martian_derive::MartianType;

#[derive(MartianType)]
enum Invalid {
	StrVariant,
	MapVariant(u32),
	AnotherVariant { f: i32 }
}

fn main() {}
