#include "ThreadSafeQueue.cpp"
#include <chrono>
#include <iostream>

int sumar(int a, int b) {
  std::this_thread::sleep_for(std::chrono::seconds(1));
  return a + b;
}

int main() {
  // Crear un pool con 4 hilos de trabajo
  ThreadPool pool(4);

  // Enviar tareas de forma asíncrona y capturar los futuros
  std::future<int> resultado1 = pool.enqueue(sumar, 5, 10);
  std::future<int> resultado2 = pool.enqueue(sumar, 20, 30);

  // Obtener los resultados (el hilo principal espera si aún no terminan)
  std::cout << "Resultado 1: " << resultado1.get() << "\n";
  std::cout << "Resultado 2: " << resultado2.get() << "\n";

  return 0; // El destructor del pool se ejecuta automáticamente aquí de forma
            // segura
}
