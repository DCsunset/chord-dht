use chord_rust::chord;

fn main() {
	let node = chord::node::Node::new();
	println!("Created node: {}", node.id);
}
