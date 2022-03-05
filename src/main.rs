use chord_rust::chord;

fn main() {
	let node = chord::node::Node {
		id: 0
	};
	println!("Created node: {}", node.id);
}
